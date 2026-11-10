# Copyright 2023-2026 Airbus, CS Group
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Diagnostic of the dask task failures caused by dead dask workers.

When the dask workers running a task die (e.g. the worker pod is killed by Kubernetes when it exceeds its
memory limit), the task fails with a KilledWorker exception that doesn't tell why. Run from the
rs-dpr-service side, this module reads the dask scheduler event log to explain the worker deaths.

The real cause is only known by Kubernetes (pod status OOMKilled, Evicted, ...), which the rs-dpr-service
cannot read: the explanations below are the probable causes deduced from the dask events.
"""

from datetime import UTC, datetime
from typing import Any

from distributed import KilledWorker
from distributed.client import Client as DaskClient

from rs_dpr_service.dask.call_dask import format_bytes
from rs_dpr_service.utils.logging import Logging

logger = Logging.default(__name__)

# Probable causes of a worker removal, by prefix of the scheduler stimulus_id
_REMOVAL_CAUSES = {
    "handle-worker-cleanup": (
        "the connection with the worker was lost: the worker process or its pod was killed or stopped, e.g. "
        "OOMKilled by Kubernetes when the pod exceeds its memory limit, or the pod was evicted for lack of "
        "ephemeral storage (disk) or node resources"
    ),
    "check-worker-ttl": (
        "the worker stopped sending heartbeats: it was frozen or unreachable, e.g. CPU starvation, swapping, "
        "or network issue"
    ),
    "retire-workers": "the worker was retired, e.g. by the cluster scale down",
}

_MEMORY_RESTART_CAUSE = "restarted by the dask nanny because the worker memory exceeded the dask memory budget"

_HINT = (
    "Note: the memory of the processing subprocesses is not monitored by dask, only the worker pod memory limit "
    "applies. To confirm the cause, check the status of the worker pods in Kubernetes "
    "(lastState.terminated.reason: OOMKilled, status.reason: Evicted)."
)


def _format_time(timestamp: float) -> str:
    """Format a dask event timestamp"""
    return datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _removal_cause(stimulus_id: str) -> str:
    """Return the probable cause of a worker removal from its scheduler stimulus id"""
    for prefix, cause in _REMOVAL_CAUSES.items():
        if stimulus_id.startswith(prefix):
            return cause
    return f"unknown cause (stimulus_id={stimulus_id!r})"


def _get_events(dask_client: DaskClient, topic: str) -> list[tuple[float, dict[str, Any]]]:
    """Return the scheduler events of a topic, as a list of (timestamp, message)"""
    return [(timestamp, msg) for timestamp, msg in dask_client.get_events(topic) if isinstance(msg, dict)]


def explain_task_failure(dask_client: DaskClient | None, exception: BaseException) -> str:
    """
    Explain why the dask workers running a failed task died.

    Args:
        dask_client: dask client connected to the cluster that ran the task
        exception: exception raised by the dask future

    Returns:
        The explanation, to append to the error message, or an empty string if the task failure was not caused
        by dead workers or if the explanation cannot be retrieved.
    """
    if not dask_client or not isinstance(exception, KilledWorker):
        return ""

    lines = [f"Diagnostic of the dask workers that died while running the task {exception.task!r}:"]
    last_worker = exception.last_worker
    memory_limit = getattr(last_worker, "memory_limit", None)
    if memory_limit:
        lines.append(f"Dask worker memory limit: {format_bytes(memory_limit)}")

    try:
        removals = [
            (timestamp, msg)
            for timestamp, msg in _get_events(dask_client, "all")
            if msg.get("action") == "remove-worker" and exception.task in msg.get("processing-tasks", ())
        ]
        dead_workers = {msg.get("worker") for _, msg in removals}

        # Workers restarted by their nanny because they exceeded the dask memory budget
        memory_restarts = {
            msg.get("worker"): msg
            for _, msg in _get_events(dask_client, "worker-restart-memory")
            if msg.get("worker") in dead_workers
        }
    except Exception as error:  # pylint: disable=broad-exception-caught
        logger.warning(f"Cannot read the dask scheduler events: {error}")
        lines.append(f"Cannot read the dask scheduler events: {error}")
        lines.append(_HINT)
        return "\n".join(lines)

    if not removals:
        lines.append("No worker removal found in the dask scheduler events.")

    # Group the dead workers by cause, keep the chronological order
    causes: dict[str, list[str]] = {}
    for timestamp, msg in removals:
        worker = msg.get("worker")
        detail = f"{worker} at {_format_time(timestamp)}"
        if worker in memory_restarts:
            cause = _MEMORY_RESTART_CAUSE
            detail += f" (memory: {format_bytes(memory_restarts[worker].get('rss', 0))})"
        else:
            cause = _removal_cause(str(msg.get("stimulus_id", "")))
        causes.setdefault(cause, []).append(detail)
    for cause, workers in causes.items():
        lines.append(f"- {len(workers)} worker(s): {cause}. Workers: {', '.join(workers)}")

    lines.append(_HINT)
    return "\n".join(lines)
