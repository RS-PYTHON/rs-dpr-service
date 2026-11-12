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

"""Tests for rs_dpr_service.dask.worker_diagnostics."""

from types import SimpleNamespace
from typing import Any, cast

import pytest
from distributed import KilledWorker

from rs_dpr_service.dask.worker_diagnostics import explain_task_failure

TASK = "convert_safe_to_zarr-c6aa9c70bc1b6e34d9d2cd4153c3aa9a"

# 2026-11-10T11:04:00Z
T0 = 1794308640.0


def _killed_worker(memory_limit: int | None = 8 * 1024**3) -> KilledWorker:
    """Build the KilledWorker exception raised by a dask future, as the scheduler does."""
    last_worker = SimpleNamespace(address="tls://w4", memory_limit=memory_limit)
    return KilledWorker(task=TASK, last_worker=cast(Any, last_worker), allowed_failures=3)


def _removal(worker: str, stimulus_id: str, task: str = TASK) -> dict:
    """Build a 'remove-worker' event message of the dask scheduler"""
    return {"action": "remove-worker", "worker": worker, "processing-tasks": [task], "stimulus_id": stimulus_id}


def _dask_client(mocker, events: dict[str, list]):
    """Mock a dask client whose scheduler has the given events by topic"""
    client = mocker.Mock()
    client.get_events.side_effect = lambda topic: tuple(events.get(topic, ()))
    return client


def test_explain_task_failure_ignores_the_other_failures(mocker):
    """Only the dead workers are explained: the task errors already contain their own traceback."""
    client = _dask_client(mocker, {})

    assert explain_task_failure(client, RuntimeError("boom")) == ""
    assert explain_task_failure(None, _killed_worker()) == ""
    client.get_events.assert_not_called()


def test_explain_task_failure_explains_killed_pods(mocker):
    """Test the workers killed without notice (e.g. OOMKilled pods) are grouped by cause."""
    client = _dask_client(
        mocker,
        {
            "all": [
                (T0 - 60, {"action": "add-worker", "worker": "tls://w1"}),
                (T0, _removal("tls://w1", "handle-worker-cleanup-1793790240.0")),
                (T0 + 1, _removal("tls://other", "handle-worker-cleanup-1793790241.0", task="other-task")),
                (T0 + 60, _removal("tls://w2", "handle-worker-cleanup-1793790300.0")),
                (T0 + 120, _removal("tls://w3", "check-worker-ttl-1793790360.0")),
                (T0 + 180, _removal("tls://w4", "something-new-1793790420.0")),
                (T0 + 190, "not a dict message"),
            ],
        },
    )

    diagnostic = explain_task_failure(client, _killed_worker())

    lines = diagnostic.split("\n")
    assert lines[0] == f"Diagnostic of the dask workers that died while running the task {TASK!r}:"
    assert lines[1] == "Dask worker memory limit: 8.0 GiB"
    assert lines[2].startswith("- 2 worker(s): the connection with the worker was lost")
    assert "OOMKilled by Kubernetes" in lines[2]
    assert lines[2].endswith("Workers: tls://w1 at 2026-11-10T11:04:00Z, tls://w2 at 2026-11-10T11:05:00Z")
    assert lines[3].startswith("- 1 worker(s): the worker stopped sending heartbeats")
    assert lines[4] == (
        "- 1 worker(s): unknown cause (stimulus_id='something-new-1793790420.0'). "
        "Workers: tls://w4 at 2026-11-10T11:07:00Z"
    )
    assert lines[5].startswith("Note: the memory of the processing subprocesses is not monitored by dask")
    # The workers that ran other tasks are ignored
    assert "tls://other" not in diagnostic


def test_explain_task_failure_explains_the_nanny_memory_restarts(mocker):
    """Test the workers restarted by the dask nanny for exceeding the memory budget."""
    client = _dask_client(
        mocker,
        {
            "all": [(T0, _removal("tls://w1", "handle-worker-cleanup-1793790240.0"))],
            "worker-restart-memory": [
                (T0 - 1, {"worker": "tls://w1", "pid": 12, "rss": 7.8 * 1024**3}),
                (T0 - 1, {"worker": "tls://other", "pid": 13, "rss": 1024}),
            ],
        },
    )

    diagnostic = explain_task_failure(client, _killed_worker(memory_limit=None))

    assert "memory limit" not in diagnostic.split("\n")[1]
    assert (
        "- 1 worker(s): restarted by the dask nanny because the worker memory exceeded the dask memory budget. "
        "Workers: tls://w1 at 2026-11-10T11:04:00Z (memory: 7.8 GiB)"
    ) in diagnostic


def test_explain_task_failure_without_removal_events(mocker):
    """The scheduler event log is bounded: the old events may be missing."""
    diagnostic = explain_task_failure(_dask_client(mocker, {"all": []}), _killed_worker())

    assert "No worker removal found in the dask scheduler events." in diagnostic


@pytest.mark.parametrize("error", [OSError("scheduler unreachable"), TimeoutError("timeout")])
def test_explain_task_failure_never_fails(mocker, error):
    """The diagnostic is best effort: it must never hide the original error."""
    client = mocker.Mock()
    client.get_events.side_effect = error

    diagnostic = explain_task_failure(client, _killed_worker())

    assert f"Cannot read the dask scheduler events: {error}" in diagnostic
    assert "Note: the memory of the processing subprocesses" in diagnostic
