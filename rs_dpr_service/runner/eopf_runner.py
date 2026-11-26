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

"""Run an EOPF processor in a dedicated driver pod."""

import base64
import json
import logging
import os
import os.path as osp
import re
import shutil
import signal
import socket
import subprocess
import time
import traceback
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from importlib import reload
from importlib.metadata import distributions
from pathlib import Path
from typing import Any

import yaml
from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import Status, StatusCode
from opentelemetry.trace.span import Span, SpanContext
from packaging.version import Version

from rs_dpr_service.utils import settings
from rs_dpr_service.utils.init_opentelemetry import (
    init_traces,
    record_error,
    start_span,
)
from rs_dpr_service.utils.logging import EOPF_OUTPUT_LOG_ATTR
from rs_dpr_service.utils.settings import ExperimentalConfig

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Number of last subprocess output lines repeated in the error message when the subprocess fails
LOG_TAIL_SIZE = 20

# cgroup files with the memory limit of the container (cgroup v2, then v1)
CGROUP_MEMORY_LIMIT_FILES = ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes")

# Above this value, a cgroup v1 memory limit means "no limit"
_CGROUP_V1_NO_LIMIT = 2**60

# Warnings hidden in the EOPF subprocesses (PYTHONWARNINGS syntax).
# EOPF evaluates the formatter parameters of its mapping files, that contain regexes, with ast.literal_eval():
# Python warns about their escape sequences (e.g. '\.') at each evaluation, thousands of times per product,
# which floods the job logs.
EOPF_IGNORED_WARNINGS = "ignore:invalid escape sequence:SyntaxWarning"


def ignore_eopf_warnings(env: dict[str, str]) -> dict[str, str]:
    """Add the EOPF_IGNORED_WARNINGS to the PYTHONWARNINGS of a subprocess environment, return the environment."""
    env["PYTHONWARNINGS"] = ",".join(filter(None, (env.get("PYTHONWARNINGS"), EOPF_IGNORED_WARNINGS)))
    return env


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
            ": probably out of memory (killed by the Linux OOM killer when the driver pod exceeds its memory "
            "limit), or forced termination"
        )
    elif signum == signal.SIGTERM:
        description += ": terminated from outside, e.g. the driver pod was evicted or deleted"
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


def log_driver_resources(job_id: str, directory: str):
    """
    Log the driver container memory limit and free disk space.

    Args:
        job_id: job id
        directory: working directory, on the disk where the processing writes its local files
    """
    try:
        memory_limit = get_memory_limit()
        memory = format_bytes(memory_limit) if memory_limit else "no limit"
        disk = shutil.disk_usage(directory)
        logger.info(
            f"[JOB:{job_id}] Driver {get_ip_address()!r} resources: container memory limit: {memory}, "
            f"free disk space in {directory!r}: {format_bytes(disk.free)} / {format_bytes(disk.total)}",
        )
    except Exception as exception:  # pylint: disable=broad-exception-caught
        logger.debug(f"[JOB:{job_id}] Cannot read the driver resources: {exception}")


def forward_subprocess_output(
    proc: subprocess.Popen,
    job_id: str,
    on_line: Callable[[str], str] | None = None,
) -> deque[str]:
    """Forward subprocess output to the pod logs and return the last non-empty lines."""
    tail: deque[str] = deque(maxlen=LOG_TAIL_SIZE)
    if proc.stdout:
        for line in proc.stdout:
            if on_line:
                line = on_line(line)
            line = line.rstrip()
            if line:
                logger.info(f"[JOB:{job_id}] {line}", extra={EOPF_OUTPUT_LOG_ATTR: True})
                tail.append(line)
    return tail


def format_output_tail(tail: deque[str] | list[str]) -> str:
    """Return the last subprocess output lines to append to an error message."""
    if not tail:
        return " No output."
    return f" Last {len(tail)} output lines:\n" + "\n".join(tail)


def get_ip_address() -> str:
    """Return IP address, see: https://stackoverflow.com/a/166520"""
    return socket.gethostbyname(socket.gethostname())


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


class ProcessorCaller:
    """Run the DPR processor in the driver pod."""

    def __init__(
        self,
        caller_env: dict[str, str],
        span_context: SpanContext | None,
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
        self.proc: subprocess.Popen | None = None

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

    def run_processor(self) -> dict:
        """Run the processor, forwarding SIGTERM to the EOPF subprocess."""
        previous_handler = signal.signal(signal.SIGTERM, self._handle_sigterm)
        try:
            return self._run_processor()
        finally:
            signal.signal(signal.SIGTERM, previous_handler)

    def _handle_sigterm(self, _signum, _frame):
        self.cancelled = True
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.terminate()
            except ProcessLookupError:
                pass

    def _run_processor(self) -> dict:
        self.copy_caller_env()

        # Init opentelemetry and record all task in an Opentelemetry span
        init_traces()
        with start_span(__name__, f"[{self.cluster_info.cluster_label}] dpr_dask_processor", self.span_context) as span:
            try:
                # This should run on the dask worker
                logger.debug(f"[JOB:{self.job_id}] Call 'ProcessorCaller.run' from {get_ip_address()!r}")

                packages = sorted(f"{dist.metadata['Name']}=={dist.version}" for dist in distributions())
                logger.debug(
                    f"[JOB:{self.job_id}] " f"Python package versions:\n{json.dumps(packages, indent=2)}",
                )
                self.init()
                log_driver_resources(self.job_id, self.working_dir)

                if self.processor_name == "mockup":
                    # TODO: test this on the deployed cluster
                    logger.debug(f"[JOB:{self.job_id}] DPR Mockup processor debug message")
                    logger.info(f"[JOB:{self.job_id}] DPR Mockup processor info message")
                    logger.warning(f"[JOB:{self.job_id}] DPR Mockup processor warning message")

                start_time = time.time()
                if self.cancelled:
                    raise RuntimeError("Processing cancelled before the EOPF subprocess started")
                self.trigger()
                self.exec_times.append(("Run processor", time.time() - start_time))
                if self.cancelled:
                    raise RuntimeError("Processing cancelled by the user")
                result = self.finalize()
                if self.cancelled:
                    raise RuntimeError("Processing cancelled by the user")
                return result

            # In all cases, run the finalize function
            except Exception as e:  # pylint: disable=broad-exception-caught
                try:
                    self.finalize(upload_outputs=False)
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
                elif hasattr(downloaded_dir, "path"):
                    local_config_dir = downloaded_dir.path
                else:
                    raise AttributeError("Downloaded AnyPath has neither 'fs_path' nor 'path'")
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
            # pylint: disable-next=no-name-in-module
            from eopf.cli.cli_triggering_triggers import (  # type: ignore[attr-defined]
                validate_payload_command,
            )

            # EOPF's CLI uses basicConfig(force=True), which closes Dask's root handlers.
            root_logger = logging.getLogger()
            handlers = root_logger.handlers[:]
            level = root_logger.level
            for handler in handlers:
                root_logger.removeHandler(handler)
            try:
                validate_payload_command.main(
                    args=[payload_file, "--level", "preflight"],
                    standalone_mode=False,  # raise instead of sys.exit()
                )
            finally:
                for handler in root_logger.handlers[:]:
                    root_logger.removeHandler(handler)
                    if handler not in handlers:
                        handler.close()
                for handler in handlers:
                    root_logger.addHandler(handler)
                root_logger.setLevel(level)

    def customize_payload_file(self, payload_file: str):
        """Customize the payload file values"""

        # Read the payload file contents
        with open(payload_file, encoding="utf-8") as opened:
            payload_contents = yaml.safe_load(opened)

        for options in self._collect_storage_options(payload_contents):
            for value in (options.get("key"), options.get("secret"), *options.get("client_kwargs", {}).values()):
                if isinstance(value, str) and value and set(value) == {"*"}:
                    raise ValueError("Payload contains masked S3 storage options; use the original payload")

        self.set_payload_s3_secrets(payload_contents)

        # Write the Dask context configuration in the payload file.
        self.write_dask_context(payload_contents)

        # Handle the experimental configuration
        self.handle_experimental_config(payload_contents)

        # The payload contains credentials and the Gateway token.
        os.chmod(payload_file, 0o600)
        with open(payload_file, "w+", encoding="utf-8") as opened:
            opened.write(yaml.safe_dump(payload_contents))

        # Display the payload file contents in the log and log file
        with open(payload_file, encoding="utf-8") as opened:

            self.payload_contents = yaml.safe_load(opened)
            message = f"Dask cluster label: {self.cluster_info.cluster_label!r}\n"
            message += f"Dask cluster instance: {self.cluster_info.cluster_instance!r}\n"
            message += f"Payload file: {payload_file!r}\n"

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

        fd = os.open(secret_conf_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
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

    def trigger(self):
        """Trigger eopf-cpm execution"""

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
        if self.cancelled:
            raise RuntimeError("Processing cancelled before the EOPF subprocess started")
        with subprocess.Popen(
            self.command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=self.working_dir,
            env=env,
        ) as proc:
            self.proc = proc
            if self.cancelled:
                self._handle_sigterm(signal.SIGTERM, None)
            span.set_attribute("subprocess.pid", proc.pid)
            # Write output to a log file and string + redirect to the prefect logger
            with open(self.log_path, "a", encoding="utf-8") as log_file:

                def on_line(line: str) -> str:
                    # Hide secrets from logs, write them to the log file
                    line = self.hide_secrets(line)
                    log_file.write(line)
                    return line

                tail = forward_subprocess_output(proc, self.job_id, on_line)

            # Wait for the execution to finish
            status_code = proc.wait()

            span.set_attribute("subprocess.returncode", status_code)

            self.proc = None

            # Raise exception if the eopf status code is != 0
            if status_code or self.cancelled:
                cause = "cancelled by the user" if self.cancelled else describe_exit_code(status_code)
                err_msg = f"EOPF error, {cause}, please see the log.{format_output_tail(tail)}"
                span.set_status(Status(StatusCode.ERROR, err_msg))
                raise RuntimeError(err_msg)

            span.set_status(Status(StatusCode.OK))
            logger.info(f"[JOB:{self.job_id}] EOPF finished successfully with status code {status_code!r}")

    def _prepare_env_with_trace_context(self) -> dict[str, str]:
        # https://oneuptime.com/blog/post/2026-02-06-trace-python-subprocess-calls-opentelemetry/view#context-propagation-to-child-processes
        # https://opentelemetry.io/docs/languages/python/propagation/#manual-context-propagation
        env = ignore_eopf_warnings(os.environ.copy())

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

    def finalize(self, upload_outputs: bool = True) -> dict:
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
            for credentials, local_path, s3_path in self.to_be_uploaded if upload_outputs else []:
                logger.info(f"[JOB:{self.job_id}] Upload {local_path!r} to {s3_path!r}")
                try:
                    credentials.rm(s3_path, recursive=True)  # remove existing from s3 bucket
                except FileNotFoundError:
                    pass
                credentials.put(str(local_path), s3_path, recursive=True)

            if self.to_be_uploaded:
                self.exec_times.append(("Upload output files", time.time() - start_time))

        except Exception:
            logger.exception(f"[JOB:{self.job_id}] Finalization failed")
            raise

        for description, exec_time in self.exec_times:
            logger.info(f"[JOB:{self.job_id}] [TIME] {description}: {str(timedelta(seconds=exec_time))}")

        # NOTE: with the real processor, what should we return ?
        return {}


def main():
    """Read the Job environment and run the processor."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    env = dict(os.environ)
    required: tuple[str, ...] = (
        "JOB_ID",
        "PROCESSOR_NAME",
        "S3_CONFIG_DIR",
        "PAYLOAD_SUBPATH",
        "S3_REPORT_DIR",
        "S3_ACCESSKEY",
        "S3_SECRETKEY",
        "S3_ENDPOINT",
        "S3_REGION",
        "DASK_GATEWAY_ADDRESS",
        "DASK_CLUSTER_INSTANCE",
        "DASK_CLUSTER_LABEL",
    )
    if settings.LOCAL_MODE:
        required += ("LOCAL_DASK_USERNAME", "LOCAL_DASK_PASSWORD")
    else:
        required += ("JUPYTERHUB_API_TOKEN",)
    missing = [key for key in required if not env.get(key)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    carrier = {key.lower(): env[key] for key in ("TRACEPARENT", "TRACESTATE") if env.get(key)}
    parent = trace.get_current_span(extract(carrier)).get_span_context()
    caller = ProcessorCaller(
        caller_env=env,
        span_context=parent if parent.is_valid else None,
        dask_gateway_address=env["DASK_GATEWAY_ADDRESS"],
        cluster_info=ClusterInfo(
            jupyter_token=env.get("JUPYTERHUB_API_TOKEN", ""),
            dask_gateway_address=env["DASK_GATEWAY_ADDRESS"],
            cluster_label=env["DASK_CLUSTER_LABEL"],
            cluster_instance=env["DASK_CLUSTER_INSTANCE"],
        ),
        processor_name=env["PROCESSOR_NAME"],
        job_id=env["JOB_ID"],
        data={
            "s3_config_dir": env["S3_CONFIG_DIR"],
            "payload_subpath": env["PAYLOAD_SUBPATH"],
            "s3_report_dir": env["S3_REPORT_DIR"],
        },
    )
    caller.run_processor()


if __name__ == "__main__":
    main()
