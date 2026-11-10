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
This module contains the code that is related to dask and/or sent to the dask workers.
Avoid import unnecessary dependencies here.
"""

import base64
import hashlib
import importlib
import json
import logging
import os
import os.path as osp
import re
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid
import zipfile
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from importlib import reload
from io import BytesIO, TextIOWrapper
from pathlib import Path
from typing import Any

import distributed
import yaml
from distributed.client import Client as DaskClient
from distributed.diagnostics.plugin import SchedulerPlugin, WorkerPlugin
from distributed.worker import get_client
from opentelemetry.propagate import inject
from opentelemetry.trace import Status, StatusCode
from opentelemetry.trace.span import Span, SpanContext
from packaging.version import Version
from pip._internal.operations import freeze

from rs_dpr_service.utils import settings
from rs_dpr_service.utils.init_opentelemetry import (
    init_traces,
    record_error,
    start_span,
)
from rs_dpr_service.utils.logging import EOPF_OUTPUT_LOG_ATTR
from rs_dpr_service.utils.settings import CANCEL_JOB, ExperimentalConfig

# Don't use rs_dpr_service.utils.logging, it's not forwarded to the client
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Env vars of the S3 bucket credentials, used by the SAFE to Zarr conversion
S3_ENV_VARS = ("S3_ACCESSKEY", "S3_SECRETKEY", "S3_ENDPOINT", "S3_REGION")

# Number of subprocess output lines merged into a single forwarded log record, and the
# max time a buffered line waits before being flushed anyway.
LOG_BATCH_SIZE = 50
LOG_BATCH_INTERVAL_SECONDS = 1

# Number of last subprocess output lines repeated in the error message when the subprocess fails
LOG_TAIL_SIZE = 20

# cgroup files with the memory limit of the container (cgroup v2, then v1)
CGROUP_MEMORY_LIMIT_FILES = ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes")

# Above this value, a cgroup v1 memory limit means "no limit"
_CGROUP_V1_NO_LIMIT = 2**60


def format_bytes(size: float) -> str:
    """Return a human readable size, e.g. '8.0 GiB'"""
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(size) < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TiB"


def describe_exit_code(returncode: int) -> str:
    """
    Describe a subprocess exit code, with the probable cause when the subprocess was killed by a signal.

    Args:
        returncode: subprocess return code. Negative when killed by a signal (subprocess convention),
            or 128 + the signal number when killed under a shell.
    """
    if returncode < 0:
        signum = -returncode
    elif 128 < returncode < 128 + signal.NSIG:
        signum = returncode - 128
    else:
        return f"exit code {returncode}"

    try:
        name = signal.Signals(signum).name
    except ValueError:
        name = f"signal {signum}"
    description = f"killed by {name} (exit code {returncode})"
    if signum == signal.SIGKILL:
        description += (
            ": probably out of memory (killed by the Linux OOM killer when the dask worker pod exceeds its memory "
            "limit), or forced termination"
        )
    elif signum == signal.SIGTERM:
        description += ": terminated from outside, e.g. the dask worker pod was evicted or deleted"
    elif signum == signal.SIGSEGV:
        description += ": segmentation fault, crash in native code"
    return description


def get_memory_limit() -> int | None:
    """Return the memory limit in bytes of the container from its cgroup, or None if no limit or unknown."""
    for path in CGROUP_MEMORY_LIMIT_FILES:
        try:
            value = Path(path).read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if value == "max":
            return None
        try:
            limit = int(value)
        except ValueError:
            continue
        return limit if limit < _CGROUP_V1_NO_LIMIT else None
    return None


def log_worker_resources(job_id: str, directory: str):
    """
    Log the memory limit and free disk space of the dask worker, to help diagnose the worker deaths.

    Args:
        job_id: job id
        directory: working directory, on the disk where the processing writes its local files
    """
    try:
        memory_limit = get_memory_limit()
        memory = format_bytes(memory_limit) if memory_limit else "no limit"
        disk = shutil.disk_usage(directory)
        logger.info(
            f"[JOB:{job_id}] Dask worker {get_ip_address()!r} resources: container memory limit: {memory}, "
            f"free disk space in {directory!r}: {format_bytes(disk.free)} / {format_bytes(disk.total)}",
        )
    except Exception as exception:  # pylint: disable=broad-exception-caught
        logger.debug(f"[JOB:{job_id}] Cannot read the dask worker resources: {exception}")


def _should_flush_log_batch(batch: list[str], last_flush: float) -> bool:
    """Return True if the buffered batch is full, or non-empty and waiting long enough."""
    if not batch:
        return False
    if len(batch) >= LOG_BATCH_SIZE:
        return True
    return (time.monotonic() - last_flush) >= LOG_BATCH_INTERVAL_SECONDS


def _flush_log_batch(job_id: str, batch: list[str]) -> None:
    """Log buffered subprocess output lines as a single record, then clear the buffer."""
    if batch:
        joined = "\n".join(batch)
        # The EOPF lines already have their own "date - logger - LEVEL - " prefix: flag the record
        # so the rs-dpr-service log stream sends them as they are.
        logger.info(f"[JOB:{job_id}] {joined}", extra={EOPF_OUTPUT_LOG_ATTR: True})
        batch.clear()


def forward_subprocess_output(
    proc: subprocess.Popen,
    job_id: str,
    on_line: Callable[[str], str] | None = None,
) -> deque[str]:
    """
    Forward the subprocess output (proc.stdout, in text mode) to the job log stream, live.

    The lines are forwarded by batches: one dask log record per line floods the rs-dpr-service caller
    (Client.forward_logging() re-processes every record live). The lines forwarded before a dask worker
    death are already on the caller side, so they are not lost.

    Args:
        proc: running subprocess
        job_id: job id
        on_line: called for each raw output line (with its line ending), returns the line to forward

    Returns:
        The last LOG_TAIL_SIZE non-empty forwarded lines.
    """
    tail: deque[str] = deque(maxlen=LOG_TAIL_SIZE)

    # Use select() so we can periodically check whether the buffered batch is due for a time-based flush,
    # even while no new line has arrived yet (readline() alone would block for as long as the processor
    # stays quiet, which could delay a partial batch by a very long time).
    batch: list[str] = []
    last_flush = time.monotonic()
    while proc.stdout:
        ready, _, _ = select.select([proc.stdout], [], [], LOG_BATCH_INTERVAL_SECONDS)
        if ready:
            line = proc.stdout.readline()
            if line == "":
                break
            if on_line:
                line = on_line(line)

            # Buffer non-empty lines
            line = line.rstrip()
            if line:
                batch.append(line)
                tail.append(line)

        # Flush once the batch is full, or once it's been waiting long enough
        if _should_flush_log_batch(batch, last_flush):
            _flush_log_batch(job_id, batch)
            last_flush = time.monotonic()

    # Flush the remaining buffered lines, if any
    _flush_log_batch(job_id, batch)
    return tail


def format_output_tail(tail: deque[str] | list[str]) -> str:
    """Return the last subprocess output lines to append to an error message."""
    if not tail:
        return " No output."
    return f" Last {len(tail)} output lines:\n" + "\n".join(tail)


def get_ip_address() -> str:
    """Return IP address, see: https://stackoverflow.com/a/166520"""
    return socket.gethostbyname(socket.gethostname())


# Files of this project uploaded to the dask workers, and associated name in the zip archive
_UPLOADED_FILES = (
    "__init__.py",
    "safe_to_zarr.py",
    "dask/__init__.py",
    "dask/call_dask.py",
    "utils/__init__.py",
    "utils/init_opentelemetry.py",
    "utils/logging.py",
    "utils/settings.py",
)

# Fixed timestamp of the zip entries, so the zip content (and name) only depends on the source code
_ZIP_DATE_TIME = (1980, 1, 1, 0, 0, 0)


def _uploaded_module_names(package_name: str) -> list[str]:
    """Return the names of the modules contained in the uploaded zip, parent packages first."""
    names = []
    for relative_path in _UPLOADED_FILES:
        parts = relative_path.removesuffix(".py").split("/")
        if parts[-1] == "__init__":
            parts.pop()
        names.append(".".join([package_name, *parts]))
    return sorted(names)


def build_module_zip() -> tuple[str, bytes]:
    """
    Build the zip of this project uploaded to the dask workers.

    The zip is built in memory with fixed entry timestamps, so the same source code always gives the same
    zip content. Its name contains a hash of this content, e.g. "rs_dpr_service-0123456789abcdef.zip".

    Returns:
        zip file name and content
    """
    # Root of the current project
    root = Path(__file__).parent.parent
    package_name = root.name  # should be "rs_dpr_service"

    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zipped:
        for relative_path in _UPLOADED_FILES:
            info = zipfile.ZipInfo(f"{package_name}/{relative_path}", date_time=_ZIP_DATE_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            zipped.writestr(info, (root / relative_path).read_bytes())
    data = buffer.getvalue()
    return f"{package_name}-{hashlib.sha256(data).hexdigest()[:16]}.zip", data


def make_module_install_plugins(
    zip_filename: str,
    data: bytes,
    module_names: list[str],
) -> tuple[SchedulerPlugin, WorkerPlugin]:
    """
    Return the dask scheduler and worker plugins that install the zip of this project on the scheduler and workers.

    The scheduler needs this project too: it unpickles the task graphs, which reference its functions.

    The plugin classes are defined in this function so cloudpickle sends them by value: the dask scheduler and
    workers can unpickle them before this module is installed on them.

    On the scheduler and on each worker, the plugin:
      - writes the zip in the local directory, atomically (temporary file + rename), and only if it's missing,
        so a zip read by a running task or subprocess is never truncated nor removed;
      - puts the zip first in sys.path;
      - reloads the modules of the zip only if they were loaded from another file (e.g. a previous version).
        Modules already loaded from this zip are not reloaded, so the tasks that are running are not disturbed.

    Zips of previous versions are kept: tasks of a previous version may still use them.

    Args:
        zip_filename: zip file name, containing the hash of its content
        data: zip content
        module_names: names of the modules contained in the zip, parent packages first

    Returns:
        scheduler plugin, worker plugin
    """

    def install(local_directory: str, zip_filename: str, data: bytes, module_names: list[str]):
        """Write the zip in the local directory and import the modules from it."""
        zip_path = os.path.join(local_directory, zip_filename)
        if not (os.path.isfile(zip_path) and os.path.getsize(zip_path) == len(data)):
            tmp_path = f"{zip_path}.{uuid.uuid4().hex}.tmp"
            with open(tmp_path, "wb") as tmp_file:
                tmp_file.write(data)
                tmp_file.flush()
                os.fsync(tmp_file.fileno())
            os.replace(tmp_path, zip_path)

        if zip_path in sys.path:
            sys.path.remove(zip_path)
        sys.path.insert(0, zip_path)
        importlib.invalidate_caches()

        # Reload the modules of the zip already loaded from another file, parent packages first. The modules
        # already loaded from this zip are not reloaded: it would disturb the tasks that use them.
        for name in module_names:
            module = sys.modules.get(name)
            if module is not None and not str(getattr(module, "__file__", "") or "").startswith(zip_path + os.sep):
                importlib.reload(module)
        # Import the root package, the other modules are imported when needed
        importlib.import_module(module_names[0])

    class InstallModuleSchedulerPlugin(SchedulerPlugin):
        """Install the rs_dpr_service zip on the dask scheduler."""

        # Registering a plugin with the name of an already registered one does nothing
        idempotent = True

        def __init__(self, zip_filename: str, data: bytes, module_names: list[str]):
            self.zip_filename = zip_filename
            self.data = data
            self.module_names = module_names

        async def start(self, scheduler):
            """Install the zip on the scheduler."""
            install(scheduler.local_directory, self.zip_filename, self.data, self.module_names)

    class InstallModuleWorkerPlugin(WorkerPlugin):
        """Install the rs_dpr_service zip on a dask worker."""

        # Registering a plugin with the name of an already registered one does nothing
        idempotent = True

        def __init__(self, zip_filename: str, data: bytes, module_names: list[str]):
            self.zip_filename = zip_filename
            self.data = data
            self.module_names = module_names

        def setup(self, worker):
            """Install the zip on the worker."""
            install(worker.local_directory, self.zip_filename, self.data, self.module_names)

    return (
        InstallModuleSchedulerPlugin(zip_filename, data, module_names),
        InstallModuleWorkerPlugin(zip_filename, data, module_names),
    )


def upload_this_module(dask_client: DaskClient):
    """
    Upload this current module from the caller environment to the dask scheduler and workers.

    The zip is installed by a scheduler plugin and a worker plugin named after the zip, so after a hash of its
    content, and registered as idempotent: when several jobs run on the same cluster with the same version of this
    module, only the first one installs it, the others don't touch the dask cluster. The worker plugin is also run
    by the dask workers added later to the cluster. The plugins of the other versions of this module are
    unregistered first.

    WARNING: These modules should not import other modules that are not installed in the dask
    environment or you'll have import errors.

    Args:
        dask_client: dask client to which upload the modules.
    """
    zip_filename, data = build_module_zip()
    plugin_name = zip_filename.removesuffix(".zip")
    _unregister_stale_plugins(dask_client, plugin_name)
    scheduler_plugin, worker_plugin = make_module_install_plugins(
        zip_filename,
        data,
        _uploaded_module_names(Path(__file__).parent.parent.name),
    )
    logger.debug(f"Install {zip_filename!r} on the dask scheduler and workers")
    # Install on the scheduler first: it must be able to unpickle the task graphs sent to the workers
    dask_client.register_plugin(scheduler_plugin, name=plugin_name)
    dask_client.register_plugin(worker_plugin, name=plugin_name)


def _unregister_stale_plugins(dask_client: DaskClient, plugin_name: str):
    """
    Unregister the plugins that installed other versions of this module on the dask scheduler and workers.

    The scheduler replays all its worker plugins on each new or restarted worker. Without this cleanup, a long
    running cluster would keep:
      - the UploadFile plugins registered by client.upload_file() (one per job, named
        "<tmpdir>/rs_dpr_service.zip<uuid>") by the previous versions: replayed concurrently, they all write the
        same rs_dpr_service.zip on the new worker and fail with "EOFError: EOF read where not expected";
      - the plugins of the previous versions of this module, each one writing a zip on each new worker.

    client.upload_file() also registered a scheduler plugin with the same name, unregistered too.

    Unregistering a plugin doesn't touch the zips already written, so the running tasks of a previous version are
    not disturbed.

    Args:
        dask_client: dask client connected to the cluster
        plugin_name: name of the plugins of the current version, kept
    """
    package_name = plugin_name.rsplit("-", maxsplit=1)[0]  # "rs_dpr_service"
    legacy_name = re.compile(rf"(.*/)?{re.escape(package_name)}\.zip[0-9a-f-]{{36}}")
    previous_name = re.compile(rf"{re.escape(package_name)}-[0-9a-f]{{16}}")

    def is_stale(name: str) -> bool:
        return name != plugin_name and bool(legacy_name.fullmatch(name) or previous_name.fullmatch(name))

    def plugin_names(dask_scheduler) -> tuple[list[str], list[str]]:
        """
        Return the names of the scheduler and worker plugins (run on the scheduler).

        Local function, so cloudpickle sends it by value: the scheduler may not have this module yet.
        """
        return list(dask_scheduler.plugins), list(dask_scheduler.worker_plugins)

    scheduler_names, worker_names = dask_client.run_on_scheduler(plugin_names)
    for kind, names, unregister in (
        ("scheduler", scheduler_names, dask_client.unregister_scheduler_plugin),
        ("worker", worker_names, dask_client.unregister_worker_plugin),
    ):
        for name in filter(is_stale, names):
            logger.debug(f"Unregister the stale {kind} plugin {name!r}")
            try:
                unregister(name)
            except ValueError:
                # Already unregistered by another job connected to the same cluster
                pass


@dataclass
class ClusterInfo:
    """
    Information to connect to a DPR Dask cluster.

    Attributes:
        jupyter_token: JupyterHub API token. Only used in cluster mode, not local mode.
        dask_gateway_address: Dask gateway address, defined in Jupyter, with a different value for each organization.
        cluster_label: Dask cluster label e.g. "dask-l0"
        cluster_instance: Dask cluster instance ID (something like "dask-gateway.17e196069443463495547eb97f532834").
        If instance is empty, the DPR processor will use the first cluster with the given label.
    """

    jupyter_token: str
    dask_gateway_address: str
    cluster_label: str
    cluster_instance: str | None = ""


def convert_safe_to_zarr(cfg: dict, s3_env: dict[str, str] | None = None, job_id: str = ""):
    """
    Convert from legacy product (safe format) into Zarr format using EOPF in a subprocess.

    This runs the rs_dpr_service.safe_to_zarr module as a subprocess, passing config as JSON string.
    Its output is forwarded live to the job log stream.

    Args:
        cfg: conversion configuration
        s3_env: S3 bucket credentials env vars (see S3_ENV_VARS) from the calling service, passed to the
            subprocess because they are not set in the dask worker environment.
        job_id: job id, to forward the subprocess output to the job log stream
    """

    # Serialize the config
    cfg_str = json.dumps(cfg)

    # Find the ZIP that this code lives in
    module_path = Path(__file__).resolve()
    zip_path = Path(str(module_path).split(".zip", maxsplit=1)[0] + ".zip")
    if not zip_path.is_file():
        raise RuntimeError(f"Cannot locate rs_dpr_service.zip at {zip_path}")

    # Prepare an env that lets Python import from inside the ZIP
    env = os.environ.copy()
    env.update(s3_env or {})
    # prepend the zip onto PYTHONPATH (so zipimport will kick in)
    env["PYTHONPATH"] = str(zip_path) + os.pathsep + env.get("PYTHONPATH", "")

    # The SAFE is staged in the temporary directory
    log_worker_resources(job_id, tempfile.gettempdir())

    # The subprocess writes its result into this pipe, its output contains the logs.
    # NOTE: the result is read once the subprocess has finished: it must be smaller than the pipe buffer (64 KiB).
    result_read, result_write = os.pipe()
    with os.fdopen(result_read, encoding="utf-8") as result_file:
        try:
            # Run the converter as a module
            cmd = [sys.executable, "-m", "rs_dpr_service.safe_to_zarr", cfg_str, str(result_write)]
            logger.info(f"[JOB:{job_id}] Start the SAFE to Zarr conversion of {cfg.get('safe_uri')!r}")
            with subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
                pass_fds=(result_write,),
            ) as proc:
                # Close our copy of the write end, so the read returns at the subprocess exit
                os.close(result_write)
                result_write = -1
                tail = forward_subprocess_output(proc, job_id)
                returncode = proc.wait()
        finally:
            if result_write >= 0:
                os.close(result_write)

        if returncode != 0:
            raise RuntimeError(f"Conversion failed: {describe_exit_code(returncode)}.{format_output_tail(tail)}")
        result = result_file.read().strip()
    if not result:
        raise RuntimeError(f"Conversion failed: no result written.{format_output_tail(tail)}")
    return result


class ProcessorCaller:
    """
    Run the DPR processor.

    NOTE: All methods except __init__ are run from the dask pod.
    """

    def __init__(
        self,
        caller_env: dict[str, str],
        span_context: SpanContext,
        dask_gateway_address: str,
        cluster_info: ClusterInfo,
        processor_name: str,
        job_id: str,
        data: dict,
    ):
        """
        Constructor.

        Attributes:
            caller_env: env variables coming from the caller
            span_context: OpenTelemetry caller span context
            dask_gateway_address: Dask Gateway address
            cluster_info: Information to connect to a DPR Dask cluster.
            processor_name: processor name used as opentelemetry service name
            job_id: pygeoapi job id saved in the database.
            data: data to send to the processor
            s3: Bucket access to read/write configuration and report files
            local_report_dir: Report directory on the local disk
            s3_report_dir: Report directory on the S3 bucket
            experimental_config: Experimental configuration, used only for testing
            payload_contents: Payload file contents
            command: Command used to trigger eopf-cpm
            log_path: Path of the logging file on local disk
            working_dir: Working directory on local disk
            to_be_uploaded: Output products to be uploaded to the s3 bucket at the end of the processor
            exec_times: Execution times with their description
            cancelled: True when the EOPF subprocess is terminated by a job cancellation
        """
        # NOTE: some imports exists in the dask worker environment, not in the rs-dpr-service env,
        # so we cannot import them from the top of this module.
        import s3fs  # pylint: disable=import-outside-toplevel

        # This should run on the rs-dpr-service container
        logger.debug(f"[JOB:{job_id}] Call 'ProcessorCaller.__init__' from {get_ip_address()!r}")

        self.caller_env: dict[str, str] = caller_env
        self.span_context = span_context
        self.dask_gateway_address: str = dask_gateway_address
        self.cluster_info: ClusterInfo = cluster_info
        self.processor_name: str = processor_name
        self.job_id: str = job_id
        self.data: dict = data
        self.s3: Any | None = None  # AnyPath
        self.local_report_dir: str = ""
        self.s3_report_dir: str = ""
        self.experimental_config: ExperimentalConfig | None = None
        self.payload_contents: dict = {}
        self.command: list = []
        self.log_path: str = ""
        self.working_dir: str = ""
        self.to_be_uploaded: list[tuple[s3fs.S3FileSystem, str, str]] = []
        self.exec_times: list[tuple[str, float]] = []
        self.cancelled: bool = False  # True when the EOPF subprocess is terminated by a job cancellation

    def copy_caller_env(self):
        """
        Copy environment variables from the calling service environment to the dask client.
        This function is run from inside the dask pod.
        """
        # Copy env vars from the caller
        keys = [
            "RSPY_LOCAL_MODE",
            "S3_ACCESSKEY",
            "S3_SECRETKEY",
            "S3_ENDPOINT",
            "S3_REGION",
            "PREFECT_BUCKET_NAME",
            "PREFECT_BUCKET_FOLDER",
            # Opentelemetry
            "OTEL_SERVICE_NAME",
            "TEMPO_ENDPOINT",
            "TRACEPARENT",
            "TRACESTATE",
            # Only in local mode
            "LOCAL_DASK_USERNAME",
            "LOCAL_DASK_PASSWORD",
            # Vars from ~/.s3cfg, when debugging a processor locally in local mode
            "access_key",
            "bucket_location",
            "host_base",
            "host_bucket",
            "secret_key",
        ]

        # Copy our environment variables
        for key in keys:
            if value := self.caller_env.get(key):
                os.environ[key] = value
        # Copy all OpenTelemetry environment variables
        for key, value in self.caller_env.items():
            if key.startswith("OTEL_"):
                os.environ[key] = value

        # Debug gRPC Connectivity
        # https://opentelemetry.io/docs/zero-code/python/troubleshooting/#grpc-connectivity
        # os.environ["GRPC_VERBOSITY"] = "debug"
        # os.environ["GRPC_TRACE"] = "http,call_error,connectivity_state"

        # Reload this module to read updated env vars (local/cluster mode)
        reload(settings)

        # Init AWS env
        self.set_aws_env()

    def set_aws_env(self):
        """Init the AWS environment variables from the bucket credentials."""

        os.environ["AWS_ACCESS_KEY_ID"] = os.environ["S3_ACCESSKEY"]
        os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ["S3_SECRETKEY"]
        os.environ["AWS_ENDPOINT_URL_S3"] = os.environ["S3_ENDPOINT"]
        os.environ["AWS_DEFAULT_REGION"] = os.environ["S3_REGION"]

        os.environ["AWS_DEFAULT_OUTPUT"] = "json"

        # EOPF uploads to S3 with obstore, which only allows https endpoints by default
        if os.environ["S3_ENDPOINT"].startswith("http://"):
            os.environ["AWS_ALLOW_HTTP"] = "true"
        else:
            os.environ.pop("AWS_ALLOW_HTTP", None)

    def hide_secrets(self, log: str) -> str:
        """The logs print secrets in clear e.g 'key': '<my-secret>'... so we hide them with a regex"""
        for key in (
            "key",
            "secret",
            "endpoint_url",
            "region_name",
            "api_token",
            "password",
        ):
            log = re.sub(rf"""(['"]{key}['"])\s*:\s*['"][^'"]*['"]""", r"\1: ***", log)
        return log

    def get_tasktable(
        self,
        module_name: str,
        class_name: str,
    ):
        """
        Return the DPR tasktable. This function is run from inside the dask pod.
        """
        # Copy env vars from the caller
        self.copy_caller_env()

        # Init opentelemetry and record all task in an Opentelemetry span
        init_traces()
        with start_span(__name__, f"dpr_dask_tasktable_{class_name}", self.span_context):
            # Load the python class
            class_ = getattr(importlib.import_module(module_name), class_name)

            # Get the tasktable for default mode. See:
            # https://cpm.pages.eopf.copernicus.eu/eopf-cpm/main/processor-orchestration-guide/tasktables.html#tasktables
            logger.debug(f"[JOB:{self.job_id}] Available modes for {class_}: {class_.get_available_modes()}")
            default_mode = class_.get_default_mode()
            tasktable = class_.get_tasktable_description(default_mode)
            return tasktable

    def run_processor(self) -> dict:
        """
        Run processor from the dask pod.
        """
        # Copy env vars from the caller
        self.copy_caller_env()

        # Init opentelemetry and record all task in an Opentelemetry span
        init_traces()
        with start_span(__name__, f"[{self.cluster_info.cluster_label}] dpr_dask_processor", self.span_context) as span:
            try:
                # This should run on the dask worker
                logger.debug(f"[JOB:{self.job_id}] Call 'ProcessorCaller.run' from {get_ip_address()!r}")

                # Run a pip freeze
                logger.debug(
                    f"[JOB:{self.job_id}] "
                    f"Python package versions (pip freeze):\n{json.dumps(list(freeze.freeze()), indent=2)}",
                )
                self.init()
                log_worker_resources(self.job_id, self.working_dir)

                logger.info("===== PAUSING THE PROCESS FOR ONE MINUTE, FOR TEMPORARY TEST PURPOSES =====")
                time.sleep(60)
                logger.info("===== RESUMING THE PROCESS AFTER THE PAUSE =====")

                start_time = time.time()
                self.trigger()
                self.exec_times.append(("Run processor", time.time() - start_time))

                return self.finalize()

            # In all cases, run the finalize function
            except Exception as e:  # pylint: disable=broad-exception-caught
                try:
                    self.finalize()
                except Exception:  # pylint: disable=broad-exception-caught
                    logger.exception(f"[JOB:{self.job_id}] {traceback.format_exc()}")
                record_error(span, e)
                raise

    # pylint: disable=import-outside-toplevel
    def init(self):
        """
        Init from the dask pod.
        """
        # Read arguments
        s3_config_dir = self.data["s3_config_dir"]
        payload_subpath = self.data["payload_subpath"]
        self.s3_report_dir = self.data["s3_report_dir"]

        # Check if s3_config_dir is a local path or S3 path
        is_local = not s3_config_dir.startswith(("s3://", "S3://"))

        if is_local:
            # Local path: use it directly without downloading from S3
            local_config_dir = s3_config_dir
            self.s3 = None
        else:
            # S3 path: download the configuration folder from the S3 bucket
            from eopf.common.file_utils import (  # pylint: disable=import-outside-toplevel
                AnyPath,
            )

            self.s3 = AnyPath(
                s3_config_dir,
                key=os.environ["S3_ACCESSKEY"],
                secret=os.environ["S3_SECRETKEY"],
                client_kwargs={
                    "endpoint_url": os.environ["S3_ENDPOINT"],
                    "region_name": os.environ["S3_REGION"],
                },
            )

            # Download the configuration folder from the S3 bucket into a local temp folder.
            # NOTE: AnyPath.get returns either a str with old eopf versions, or another AnyPath with newest versions.
            downloaded_dir: AnyPath | str = self.s3.get(recursive=True)
            if isinstance(downloaded_dir, AnyPath):
                if hasattr(downloaded_dir, "fs_path"):  # CPM >= 3.0.0rc4
                    local_config_dir = downloaded_dir.fs_path
                else:
                    local_config_dir = downloaded_dir.path
            else:
                local_config_dir = downloaded_dir

        logger.info(f"[JOB:{self.job_id}] The dpr processing task started in {s3_config_dir}")

        # Payload path and parent dir
        payload_file = osp.realpath(osp.join(local_config_dir, payload_subpath))
        payload_dir = osp.dirname(payload_file)

        # Change working directory
        self.working_dir = osp.join(local_config_dir, payload_dir)
        os.chdir(self.working_dir)

        # Create the reports dir
        # WARNING: fields from the payload file: dask__export_graphs, performance_report_file, ... should
        # also use this directory: ./reports
        self.local_report_dir = osp.realpath("./reports")
        self.log_path = osp.join(self.local_report_dir, Path(payload_file).with_suffix(".processor.log").name)
        shutil.rmtree(self.local_report_dir, ignore_errors=True)
        os.makedirs(self.local_report_dir, exist_ok=True)

        # Customize the payload file values
        self.customize_payload_file(payload_file)

        # Init the command that will be used to trigger the processor
        self.command = [
            "eopf_otel",
            "trigger",
            "local",
            payload_file,
        ]

        # Validate the payload file (only in newest versions)
        import eopf  # pylint: disable=import-outside-toplevel

        if Version(eopf.__version__) >= Version("3.1.0rc1"):
            from eopf.cli.cli_triggering_triggers import validate_payload_command

            validate_payload_command.main(
                args=[payload_file, "--level", "preflight"],
                standalone_mode=False,  # raise instead of sys.exit()
            )

    def customize_payload_file(self, payload_file: str):
        """Customize the payload file values"""

        # Read the payload file contents
        with open(payload_file, encoding="utf-8") as opened:
            payload_contents = yaml.safe_load(opened)

        self.set_payload_s3_secrets(payload_contents)

        # Write the Dask context configuration in the payload file.
        self.write_dask_context(payload_contents)

        # Handle the experimental configuration
        self.handle_experimental_config(payload_contents)

        # Write the payload contents back to the file
        with open(payload_file, "w+", encoding="utf-8") as opened:
            opened.write(yaml.safe_dump(payload_contents))

        # Display the payload file contents in the log and log file
        with open(payload_file, encoding="utf-8") as opened:

            self.payload_contents = yaml.safe_load(opened)
            dumped = self.hide_secrets(json.dumps(self.payload_contents, indent=2))
            message = f"Dask cluster label: {self.cluster_info.cluster_label!r}\n"
            message += f"Dask cluster instance: {self.cluster_info.cluster_instance!r}\n"
            message += f"Payload file contents: {payload_file!r}\n{dumped}\n"

            logger.debug(f"[JOB:{self.job_id}] {message}")

            with open(self.log_path, "w", encoding="utf-8") as log_file:
                log_file.write(message)

            # Get secret configuration file (optional)
            secret_conf_files = self.payload_contents.get("secret")
            if secret_conf_files:
                self.write_secret_conf_files(secret_conf_files, payload_contents, payload_file)

            # Get logging configuration file
            # log_conf_file = self.payload_contents.get("logging")

        # Patch the log config to set "disable_existing_loggers" to False else the logs are disabled.
        # This is a workaround for https://gitlab.eopf.copernicus.eu/cpm/eopf-cpm/-/issues/837
        # if log_conf_file:
        #     log_conf_file = osp.join(payload_dir, log_conf_file)
        #     logger.warning("Patching logging configuration to use"
        # "'disable_existing_loggers=False': {log_conf_file!r}")
        #     with open(log_conf_file, encoding="utf-8") as opened:
        #         log_conf_contents = yaml.safe_load(opened)
        #         log_conf_contents["disable_existing_loggers"] = False
        #     with open(log_conf_file, "w+", encoding="utf-8") as opened:
        #         opened.write(yaml.safe_dump(log_conf_contents))

    @staticmethod
    def set_payload_s3_secrets(payload_contents: dict):
        """Build processor S3 aliases from input credentials in the payload."""
        os.environ.pop("S3_SECRETS_JSON_BASE64", None)
        credentials = []
        for product in payload_contents.get("io", {}).get("input_products", []):
            params = product.get("reader_params") or product.get("store_params") or {}
            storage_options = params.get("storage_options")
            if not storage_options:
                continue
            client_kwargs = storage_options.get("client_kwargs") or {}
            credential = {
                "key": storage_options["key"],
                "secret": storage_options["secret"],
                "endpoint_url": client_kwargs["endpoint_url"],
                "region_name": client_kwargs["region_name"],
            }
            os.environ["S3_KEY"] = storage_options["key"]
            os.environ["S3_SECRET"] = storage_options["secret"]
            os.environ["S3_URL"] = client_kwargs["endpoint_url"]
            os.environ["S3_REGION"] = client_kwargs["region_name"]
            if credential not in credentials:
                credentials.append(credential)
        if not credentials:
            return
        if len(credentials) > 1:
            raise ValueError("Multiple input credential sets cannot be mapped to a single S3 alias set")

        # Later update with s3output and csinput
        data = {"s3input": credentials[0]}
        os.environ["S3_SECRETS_JSON_BASE64"] = base64.b64encode(json.dumps(data).encode()).decode()

    @staticmethod
    def _collect_storage_options(payload_contents: dict) -> list[dict]:
        io = payload_contents.get("io", {})
        result = []
        for product in io.get("input_products", []):
            if so := product.get("reader_params", product.get("store_params", {})).get("storage_options"):
                result.append(so)
        for product in io.get("output_products", []):
            if so := product.get("writer_params", product.get("store_params", {})).get("storage_options"):
                result.append(so)
        for adf in io.get("adfs", []):
            if so := adf.get("adf_params", adf.get("store_params", {})).get("storage_options"):
                result.append(so)
        return result

    def write_secret_conf_files(self, secret_conf_files: list[str], payload_contents: dict, payload_file: str):
        """
        Write the external secret json config file(s) from the ones provided in the payload.
        """
        if len(secret_conf_files) > 1:
            logger.error(f"[JOB:{self.job_id}] A single secret json config file is expected for now")
            return
        secret_conf_file = osp.join(osp.dirname(payload_file), secret_conf_files[0])

        all_storage_options = self._collect_storage_options(payload_contents)
        if not all_storage_options:
            logger.warning(
                f"[JOB:{self.job_id}] No storage_options found in payload, skipping secret conf file writing",
            )
            return

        unique_credentials = {
            (
                so.get("key"),
                so.get("secret"),
                so.get("client_kwargs", {}).get("endpoint_url"),
                so.get("client_kwargs", {}).get("region_name"),
            )
            for so in all_storage_options
        }

        if len(unique_credentials) > 1:
            logger.error(
                f"[JOB:{self.job_id}] "
                "Multiple different credential sets found in payload storage_options, "
                "cannot write a single secret conf file",
            )
            return

        key, secret, endpoint_url, region_name = unique_credentials.pop()

        with open(secret_conf_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "s3": {
                        "key": key,
                        "secret": secret,
                        "client_kwargs": {"endpoint_url": endpoint_url, "region_name": region_name},
                    },
                },
                f,
                indent=2,
            )

        logger.info(f"[JOB:{self.job_id}] Secret configuration file written: {secret_conf_file}")

    def write_dask_context(self, payload_contents: dict):
        """
        Write the Dask context configuration in the payload file.
        This Dask context is used by EOPF to connect to the Dask cluster.

        NOTE: rs-dpr-service connects to the Dask cluster, then submits an EOPF triggering to this cluster.
        Then EOPF reads the Dask context from the payload file to submit the DPR processor run to this cluster.

        We need to make sure that rs-dpr-service and EOPF use the same cluster.

        So it's safer that rs-dpr-service writes the Dask context in the payload file with the same configuration
        that is used to submit the EOPF triggering.
        """
        if settings.LOCAL_MODE:
            auth = {
                "type": "basic",
                "username": os.environ["LOCAL_DASK_USERNAME"],
                "password": os.environ["LOCAL_DASK_PASSWORD"],
            }
        else:  # cluster mode
            auth = {
                "type": "jupyterhub",
                "api_token": self.cluster_info.jupyter_token,
            }

        payload_contents.update(
            {
                "context_managers": [
                    {
                        "module": "eopf.dask_utils.dask_context_manager",
                        "context_manager": "DaskContext",
                        "parameters": {
                            "cluster_type": "gateway",
                            "cluster_config": {
                                "address": self.dask_gateway_address,
                                "reuse_cluster": self.cluster_info.cluster_instance,
                                "auth": auth,
                            },
                        },
                    },
                ],
            },
        )

    def handle_experimental_config(self, payload_contents: dict):
        """Handle the experimental configuration"""

        # Check if an experimental configuraition is set (only for testing)
        self.experimental_config = ExperimentalConfig(**self.data.get("experimental_config", {}))
        if self.experimental_config == ExperimentalConfig():
            return

        # Hard replace the dask gateway configuration with a LocalCluster
        if self.experimental_config.local_cluster.enabled:
            for manager in payload_contents["context_managers"]:
                parameters = manager.get("parameters", {})
                parameters["cluster_type"] = "local"
                if cluster_config := parameters.get("cluster_config", {}):
                    cluster_config.pop("address", None)
                    cluster_config.pop("reuse_cluster", None)
                    cluster_config.pop("auth", None)
                    cluster_config.pop("workers", None)

                    cluster_config["n_workers"] = self.experimental_config.local_cluster.n_workers
                    cluster_config["memory_limit"] = self.experimental_config.local_cluster.memory_limit
                    cluster_config["threads_per_worker"] = self.experimental_config.local_cluster.threads_per_worker

        # Read/write on the local disk rather than on the S3 bucket. Only works with a LocalCluster.
        if self.experimental_config.local_files.local_dir:

            # For each input or output product
            start_time = time.time()
            for io_key, io_value in payload_contents.get("io", {}).items():
                for product in io_value:
                    self.handle_local_product(io_key, product)
            self.exec_times.append(("Download input files", time.time() - start_time))

    def handle_local_product(self, io_key: str, original_product: dict):
        """Handle local products for the experimental configuration"""

        import s3fs  # pylint: disable=import-outside-toplevel
        from eopf.common.env_utils import (  # pylint: disable=import-outside-toplevel
            resolve_env_vars,
        )

        if not self.experimental_config:
            return

        # resolve all env_vars in the payload
        product = resolve_env_vars(original_product)

        # Get product path
        if not (s3_path := product.get("path")):
            return

        # If the path is not on a s3 bucket, this means the path is already local, so we do nothing
        if not s3_path.lower().startswith("s3:/"):
            return

        # Corresponding path on the local disk
        local_path = Path(f"{self.experimental_config.local_files.local_dir}/{s3_path[4:].lstrip('/')}")

        # Read S3 credentials
        store_params = product["store_params"]
        if store_params.get("s3_secret_alias"):
            raise RuntimeError("TODO: handle bucket credentials from a 'secrets.json' file")
        credentials = s3fs.S3FileSystem(**store_params["storage_options"])

        # Input or output product ?
        is_output = io_key == "output_products"
        is_input = not is_output

        def remove_local_path():
            """Remove product on local disk"""
            if local_path.is_file() or local_path.is_symlink():
                local_path.unlink()
            elif local_path.is_dir():
                shutil.rmtree(local_path)

        if is_input:
            # Remove the existing input product on local disk
            if self.experimental_config.local_files.overwrite_input:
                remove_local_path()

            # Download the product locally if not already there
            if not local_path.exists():
                logger.info(f"[JOB:{self.job_id}] Download {s3_path!r} to {str(local_path)!r}")
                local_path.parent.mkdir(parents=True, exist_ok=True)
                credentials.get(s3_path, local_path, recursive=True)

        if is_output:
            # Always remove the existing output product on local disk
            remove_local_path()

            # The output product will be uploaded at the end of the processor
            if self.experimental_config.local_files.upload_output:
                self.to_be_uploaded.append((credentials, str(local_path), s3_path))

        # Use the local path in the payload file
        original_product["path"] = str(local_path)

    def handle_cancellation(self, proc: subprocess.Popen, log_file: TextIOWrapper) -> distributed.Event | None:
        """Handle cancellation of an eopf task."""

        # Check that we are in a dask context
        try:
            get_client()
        except ValueError:
            # This should always be the case in cluster mode
            if settings.CLUSTER_MODE:
                raise
            # Else in local mode, just exit the method
            return None

        # Send/catch a dask distributed event for the cancellation of this job
        cancel_event = distributed.Event(CANCEL_JOB.format(job_id=self.job_id))

        def cancel_function():
            """If the cancellation event is caught, terminate the subprocess."""
            cancel_event.wait()  # go to the next line of code when the cancellation event is caught

            # If the subprocess has already finished, do nothing
            if proc.returncode is not None:
                return

            self.cancelled = True
            msg = f"\nForce termination of EOPF job: {self.job_id!r}\n"
            log_file.write(msg)
            logger.warning(f"[JOB:{self.job_id}] {msg}")

            # Call .terminate() to send a SIGTERM. This signal can be caught by eopf to do some cleaning.
            logger.warning(f"[JOB:{self.job_id}] Send SIGTERM signal")
            proc.terminate()

            # Wait a few moments, then send a SIGKILL in case the SIGTERM was not enough.
            # NOTE: the delay value should be configurable for each processor.
            # Some processors could e.g. clean directories so it could take longer to finish.
            time.sleep(60)
            logger.warning(f"[JOB:{self.job_id}] Send SIGKILL signal")
            proc.kill()

        # Run this in a separate thread.
        # Use daemon=True so we don't have to wait for the thread to finish to exit the main process.
        cancel_thread = threading.Thread(target=cancel_function, daemon=True)
        cancel_thread.start()

        return cancel_event

    def trigger(self):
        """Trigger eopf-cpm execution"""

        # Everything is run on the rs-dpr-service host machine.
        # This is used to debug in local mode / docker compose on your local machine.
        # We call eopf from python code.
        if settings.LOCAL_MODE and self.experimental_config and self.experimental_config.local_cluster.enabled:
            from eopf.triggering.runner import (  # pylint: disable=import-outside-toplevel
                EORunner,
            )

            EORunner().run(self.payload_contents)
            return

        # Trigger EOPF processing with trace context propagation, catch output
        # NOTE: we run it in a subprocess because it's easier that way to capture stdout and stderr,
        # or cancel the subprocess.
        with start_span(__name__, "eopf.subprocess") as span:
            span.set_attribute("subprocess.command", str(self.command))
            span.set_attribute("subprocess.working_dir", str(self.working_dir))
            span.set_attribute("subprocess.log_file", self.log_path)

            logger.info(
                f"[JOB:{self.job_id}] Trigger EOPF processing with command '{self.command}' "
                f"in working directory '{self.working_dir}'",
            )
            self._launch_eopf_subprocess(span, self._prepare_env_with_trace_context())

    def _launch_eopf_subprocess(self, span: Span, env: dict[str, str]):
        """Trigger eopf-cpm execution in a subprocess"""
        with subprocess.Popen(
            self.command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=self.working_dir,
            env=env,
        ) as proc:

            span.set_attribute("subprocess.pid", proc.pid)
            # Write output to a log file and string + redirect to the prefect logger
            with open(self.log_path, "a", encoding="utf-8") as log_file:

                # Handle cancellation of eopf processing
                cancel_event = self.handle_cancellation(proc, log_file)

                def on_line(line: str) -> str:
                    # Hide secrets from logs, write them to the log file
                    line = self.hide_secrets(line)
                    log_file.write(line)
                    return line

                tail = forward_subprocess_output(proc, self.job_id, on_line)

            # Wait for the execution to finish
            status_code = proc.wait()

            span.set_attribute("subprocess.returncode", status_code)

            # We don't need the cancellation thread anymore.
            # Send the cancellation event it's been waiting for, to make sure the thread will finish.
            if cancel_event:
                cancel_event.set()

            # Raise exception if the eopf status code is != 0
            if status_code:
                cause = "cancelled by the user" if self.cancelled else describe_exit_code(status_code)
                err_msg = f"EOPF error, {cause}, please see the log.{format_output_tail(tail)}"
                span.set_status(Status(StatusCode.ERROR, err_msg))
                raise RuntimeError(err_msg)

            span.set_status(Status(StatusCode.OK))
            logger.info(f"[JOB:{self.job_id}] EOPF finished successfully with status code {status_code!r}")

    def _prepare_env_with_trace_context(self) -> dict[str, str]:
        # https://oneuptime.com/blog/post/2026-02-06-trace-python-subprocess-calls-opentelemetry/view#context-propagation-to-child-processes
        # https://opentelemetry.io/docs/languages/python/propagation/#manual-context-propagation
        env = os.environ.copy()

        # Inject trace context into environment variables
        carrier: dict[str, str] = {}
        inject(carrier)

        logger.info(f"[JOB:{self.job_id}] OpenTelemetry carrier: {carrier!r}")

        # Convert carrier to environment variables
        if "traceparent" in carrier:
            env["TRACEPARENT"] = carrier["traceparent"]
        if "tracestate" in carrier:
            env["TRACESTATE"] = carrier["tracestate"]

        # Also pass as JSON for scripts that can parse it
        env["OTEL_TRACE_CONTEXT"] = json.dumps(carrier)

        # Set the opentelemetry service name
        env["OTEL_SERVICE_NAME"] = f"dpr.{self.processor_name}"

        return env

    def finalize(self) -> dict:
        """Code to run at the end of the processor."""

        try:
            # Upload the reports dir to the s3 bucket (only for S3 paths)
            # For local paths, self.s3 is None (set in init()), so this block is skipped.
            # Reports remain in the local shared disk.
            if self.s3 and self.s3_report_dir.lower().startswith("s3://"):
                logger.info(f"[JOB:{self.job_id}] Upload reports {self.local_report_dir!r} to {self.s3_report_dir!r}")
                self.s3._fs.put(  # pylint: disable=protected-access
                    self.local_report_dir,
                    self.s3_report_dir,
                    recursive=True,
                )
            else:
                logger.debug(
                    f"The upload is not performed: local path = {self.local_report_dir} |"
                    f"s3 path = {self.s3_report_dir}",
                )
            # Upload local output products to the s3 bucket
            start_time = time.time()
            for credentials, local_path, s3_path in self.to_be_uploaded:
                logger.info(f"[JOB:{self.job_id}] Upload {local_path!r} to {s3_path!r}")
                try:
                    credentials.rm(s3_path, recursive=True)  # remove existing from s3 bucket
                except FileNotFoundError:
                    pass
                credentials.put(str(local_path), s3_path, recursive=True)

            if self.to_be_uploaded:
                self.exec_times.append(("Upload output files", time.time() - start_time))

        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error(f"[JOB:{self.job_id}] {exception}")

        for description, exec_time in self.exec_times:
            logger.info(f"[JOB:{self.job_id}] [TIME] {description}: {str(timedelta(seconds=exec_time))}")

        # NOTE: with the real processor, what should we return ?
        return {}
