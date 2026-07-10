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

import contextlib
import importlib
import json
import logging
import os
import os.path as osp
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import zipfile
from dataclasses import dataclass
from datetime import timedelta
from importlib import reload
from io import TextIOWrapper
from pathlib import Path
from typing import Any

import distributed
import yaml
from distributed.client import Client as DaskClient
from distributed.worker import get_client
from opentelemetry.propagate import inject
from opentelemetry.trace import Status, StatusCode
from opentelemetry.trace.span import Span, SpanContext

from rs_dpr_service.utils import settings
from rs_dpr_service.utils.init_opentelemetry import (
    init_traces,
    record_error,
    start_span,
)
from rs_dpr_service.utils.settings import CANCEL_JOB, ExperimentalConfig

# Don't use rs_dpr_service.utils.logging, it's not forwarded to the client
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_ip_address() -> str:
    """Return IP address, see: https://stackoverflow.com/a/166520"""
    return socket.gethostbyname(socket.gethostname())


def upload_this_module(dask_client: DaskClient):
    """
    Upload this current module from the caller environment to the dask client.

    WARNING: These modules should not import other modules that are not installed in the dask
    environment or you'll have import errors.

    Args:
        clients: list of dask clients to which upload the modules.
    """
    # Root of the current project
    root = Path(__file__).parent.parent

    # Files and dirs to upload and associated name in the zip archive
    files = {
        root / "__init__.py": "rs_dpr_service/__init__.py",
        root / "safe_to_zarr.py": "rs_dpr_service/safe_to_zarr.py",
        root / "dask/__init__.py": "rs_dpr_service/dask/__init__.py",
        root / "dask/call_dask.py": "rs_dpr_service/dask/call_dask.py",
        root / "utils/__init__.py": "rs_dpr_service/utils/__init__.py",
        root / "utils/init_opentelemetry.py": "rs_dpr_service/utils/init_opentelemetry.py",
        root / "utils/logging.py": "rs_dpr_service/utils/logging.py",
        root / "utils/settings.py": "rs_dpr_service/utils/settings.py",
    }

    # We'll create a zip with our files, and upload and install it in all dask workers.
    package_name = root.name  # should be "rs_dpr_service"
    zip_filename = f"{root.name}.zip"

    # But first, if we run this function several times,
    # we need to remove the previous installed zip from the dask workers.
    def remove_previous_zip(dask_worker):
        with contextlib.suppress(FileNotFoundError):
            worker_zip_path = f"{dask_worker.local_directory}/{zip_filename}"
            os.remove(worker_zip_path)
            return f"Removed from dask worker: {worker_zip_path!r}"
        return ""

    logger.debug(json.dumps(dask_client.run(remove_previous_zip), indent=2))

    # From a temp dir
    with tempfile.TemporaryDirectory() as tmpdir:

        # Create a zip with our files
        zip_path = f"{tmpdir}/{zip_filename}"
        with zipfile.ZipFile(zip_path, "w") as zipped:

            # Zip all files
            for key, value in files.items():
                zipped.write(str(key), str(value))

        # Upload zip file to dask clients.
        # This also installs the zipped modules inside the dask python interpreter.
        try:
            dask_client.upload_file(zip_path)

        # We have this error if we scale up the number of workers.
        # But it's OK, the zip file is automatically uploaded to them anyway.
        except KeyError as e:
            logger.debug(f"Ignoring error {e}")

    # If we run this function several times, we need to reload the uploaded modules from the dask workers
    def reload_uploaded():
        reloaded = []
        for module in list(sys.modules.values()):
            if module.__name__.startswith(f"{package_name}."):
                reload(module)
                reloaded.append(module.__name__)
        return f"Reloaded: {reloaded}"

    logger.debug(json.dumps(dask_client.run(reload_uploaded), indent=2))


@dataclass
class ClusterInfo:
    """
    Information to connect to a DPR Dask cluster.

    Attributes:
        jupyter_token: JupyterHub API token. Only used in cluster mode, not local mode.
        cluster_label: Dask cluster label e.g. "dask-l0"
        cluster_instance: Dask cluster instance ID (something like "dask-gateway.17e196069443463495547eb97f532834").
        If instance is empty, the DPR processor will use the first cluster with the given label.
    """

    jupyter_token: str
    cluster_label: str
    cluster_instance: str | None = ""


def convert_safe_to_zarr(cfg):
    """
    Convert from legacy product (safe format) into Zarr format using EOPF in a subprocess.

    This runs the rs_dpr_service.safe_to_zarr module as a subprocess, passing config as JSON string.
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
    # prepend the zip onto PYTHONPATH (so zipimport will kick in)
    env["PYTHONPATH"] = str(zip_path) + os.pathsep + env.get("PYTHONPATH", "")

    # Run the converter as a module
    cmd = [sys.executable, "-m", "rs_dpr_service.safe_to_zarr", cfg_str]
    result = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Conversion failed: {result.stderr.strip()}")
    return result.stdout.strip()


class ProcessorCaller:
    """
    Run the DPR processor.

    NOTE: All methods except __init__ are run from the dask pod.
    """

    def __init__(
        self,
        caller_env: dict[str, str],
        span_context: SpanContext,
        cluster_address: str,
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
            cluster_address: Dask Gateway address
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
        """
        # NOTE: some imports exists in the dask worker environment, not in the rs-dpr-service env,
        # so we cannot import them from the top of this module.
        import s3fs  # pylint: disable=import-outside-toplevel

        # This should run on the rs-dpr-service container
        logger.debug(f"Call 'ProcessorCaller.__init__' from {get_ip_address()!r}")

        self.caller_env: dict[str, str] = caller_env
        self.span_context = span_context
        self.cluster_address: str = cluster_address
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
            logger.debug(f"Available modes for {class_}: {class_.get_available_modes()}")
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
                logger.debug(f"Call 'ProcessorCaller.run' from {get_ip_address()!r}")

                self.init()

                start_time = time.time()
                self.trigger()
                self.exec_times.append(("Run processor", time.time() - start_time))

                return self.finalize()

            # In all cases, run the finalize function
            except Exception as e:  # pylint: disable=broad-exception-caught
                try:
                    self.finalize()
                except Exception:  # pylint: disable=broad-exception-caught
                    logger.exception(traceback.format_exc())
                record_error(span, e)
                raise

    def init(self):
        """
        Init from the dask pod.
        """
        # Read arguments
        s3_config_dir = self.data["s3_config_dir"]
        payload_subpath = self.data["payload_subpath"]
        self.s3_report_dir = self.data["s3_report_dir"]

        # Get S3 file handler.
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

        logger.info(f"The dpr processing task started in {s3_config_dir}")

        # Download the configuration folder from the S3 bucket into a local temp folder.
        # NOTE: AnyPath.get returns either a str with old eopf versions, or another AnyPath with newest versions.
        local_config_dir: AnyPath | str = self.s3.get(recursive=True)
        if isinstance(local_config_dir, AnyPath):
            if hasattr(local_config_dir, "fs_path"):  # CPM >= 3.0.0rc4
                local_config_dir = local_config_dir.fs_path
            else:
                local_config_dir = local_config_dir.path

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

        self.command = [
            "eopf_otel",
            "trigger",
            "local",
            payload_file,
        ]

    def customize_payload_file(self, payload_file: str):
        """Customize the payload file values"""

        # Read the payload file contents
        with open(payload_file, encoding="utf-8") as opened:
            payload_contents = yaml.safe_load(opened)

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

            logger.debug(message)

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
    def _collect_storage_options(payload_contents: dict) -> list[dict]:
        io = payload_contents.get("I/O", payload_contents.get("io", {}))
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
            logger.error("A single secret json config file is expected for now")
            return
        secret_conf_file = osp.join(osp.dirname(payload_file), secret_conf_files[0])

        all_storage_options = self._collect_storage_options(payload_contents)
        if not all_storage_options:
            logger.warning("No storage_options found in payload, skipping secret conf file writing")
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

        logger.info("Secret configuration file written: %s", secret_conf_file)

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
                "dask_context": {
                    "cluster_type": "gateway",
                    "cluster_config": {
                        "address": self.cluster_address,
                        "reuse_cluster": self.cluster_info.cluster_instance,
                        "auth": auth,
                    },
                },
            },
        )

    def handle_experimental_config(self, payload_contents: dict):
        """Handle the experimental configuration"""

        # Check if an experimental configuraition is set (only for testing)
        self.experimental_config = ExperimentalConfig(**self.data.get("experimental_config", {}))
        if self.experimental_config == ExperimentalConfig():
            return

        # Hard replace the dask gateway configuration with a LocalCluster
        if self.experimental_config.local_cluster.enabled and (dask_context := payload_contents.get("dask_context")):
            dask_context["cluster_type"] = "local"
            if cluster_config := dask_context["cluster_config"]:
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
            for io_key, io_value in payload_contents.get("I/O", {}).items():
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
                logger.info(f"Download {s3_path!r} to {str(local_path)!r}")
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

            msg = f"\nForce termination of EOPF job: {self.job_id!r}\n"
            log_file.write(msg)
            logger.warning(msg)

            # Call .terminate() to send a SIGTERM. This signal can be caught by eopf to do some cleaning.
            logger.warning("Send SIGTERM signal")
            proc.terminate()

            # Wait a few moments, then send a SIGKILL in case the SIGTERM was not enough.
            # NOTE: the delay value should be configurable for each processor.
            # Some processors could e.g. clean directories so it could take longer to finish.
            time.sleep(60)
            logger.warning("Send SIGKILL signal")
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
                f"Trigger EOPF processing with command '{self.command}' in working directory '{self.working_dir}'",
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

                # Read eopf stdout
                while proc.stdout and (line := proc.stdout.readline()) != "":

                    # Hide secrets from logs
                    line = self.hide_secrets(line)

                    # Write to log file and string
                    log_file.write(line)

                    # Write to logger if not empty
                    line = line.rstrip()
                    if line:
                        logger.info(line)

            # Wait for the execution to finish
            status_code = proc.wait()

            span.set_attribute("subprocess.returncode", status_code)

            # We don't need the cancellation thread anymore.
            # Send the cancellation event it's been waiting for, to make sure the thread will finish.
            if cancel_event:
                cancel_event.set()

            # Raise exception if the eopf status code is != 0
            if status_code:
                err_msg = f"EOPF error, status code {status_code!r}, please see the log."
                span.set_status(Status(StatusCode.ERROR, err_msg))
                raise RuntimeError(err_msg)

            span.set_status(Status(StatusCode.OK))
            logger.info(f"EOPF finished successfully with status code {status_code!r}")

    def _prepare_env_with_trace_context(self) -> dict[str, str]:
        # https://oneuptime.com/blog/post/2026-02-06-trace-python-subprocess-calls-opentelemetry/view#context-propagation-to-child-processes
        # https://opentelemetry.io/docs/languages/python/propagation/#manual-context-propagation
        env = os.environ.copy()

        # Inject trace context into environment variables
        carrier: dict[str, str] = {}
        inject(carrier)

        logger.info(f"OpenTelemetry carrier: {carrier!r}")

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
            # Upload the reports dir to the s3 bucket
            if self.s3:
                logger.info(f"Upload reports {self.local_report_dir!r} to {self.s3_report_dir!r}")
                self.s3._fs.put(  # pylint: disable=protected-access
                    self.local_report_dir,
                    self.s3_report_dir,
                    recursive=True,
                )

            # Upload local output products to the s3 bucket
            start_time = time.time()
            for credentials, local_path, s3_path in self.to_be_uploaded:
                logger.info(f"Upload {local_path!r} to {s3_path!r}")
                try:
                    credentials.rm(s3_path, recursive=True)  # remove existing from s3 bucket
                except FileNotFoundError:
                    pass
                credentials.put(str(local_path), s3_path, recursive=True)

            if self.to_be_uploaded:
                self.exec_times.append(("Upload output files", time.time() - start_time))

        except Exception as exception:  # pylint: disable=broad-exception-caught
            logger.error(exception)

        for description, exec_time in self.exec_times:
            logger.info(f"[TIME] {description}: {str(timedelta(seconds=exec_time))}")

        # NOTE: with the real processor, what should we return ?
        return {}
