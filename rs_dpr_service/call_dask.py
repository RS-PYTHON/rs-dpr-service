# Copyright 2025 CS Group
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
import ast
import importlib
import json
import logging
import os
import os.path as osp
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import yaml
from distributed.client import Client as DaskClient
from opentelemetry.trace.span import SpanContext

from rs_dpr_service.utils import init_opentelemetry

SERVICE_NAME = "rs.dpr.dask"

# Don't use rs_dpr_service.utils.logging, it's not forwarded to the client
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def upload_this_module(dask_client: DaskClient):
    """
    Upload this current module from the caller environment to the dask client.

    WARNING: These modules should not import other modules that are not installed in the dask
    environment or you'll have import errors.

    Args:
        clients: list of dask clients to which upload the modules.
    """
    # Root of the current project
    root = Path(__file__).parent

    # Files and dirs to upload and associated name in the zip archive
    files = {
        root / "__init__.py": "rs_dpr_service/__init__.py",
        root / "call_dask.py": "rs_dpr_service/call_dask.py",
        root / "safe_to_zarr.py": "rs_dpr_service/safe_to_zarr.py",
        root / "utils/__init__.py": "rs_dpr_service/utils/__init__.py",
        root / "utils/init_opentelemetry.py": "rs_dpr_service/utils/init_opentelemetry.py",
        root / "utils/logging.py": "rs_dpr_service/utils/logging.py",
        root / "utils/utils.py": "rs_dpr_service/utils/utils.py",
    }

    # From a temp dir
    with tempfile.TemporaryDirectory() as tmpdir:

        # Create a zip with our files
        zip_path = f"{tmpdir}/{root.name}.zip"
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


def copy_caller_env(caller_env: dict[str, str]):
    """
    Copy environment variables from the calling service environment to the dask client.

    Args:
        caller_env: os.environ coming from caller
    """
    local_mode = caller_env.get("RSPY_LOCAL_MODE") == "1"

    # Copy env vars from the caller
    keys = [
        "RSPY_LOCAL_MODE",
        "S3_ACCESSKEY",
        "S3_SECRETKEY",
        "S3_ENDPOINT",
        "S3_REGION",
        "PREFECT_BUCKET_NAME",
        "PREFECT_BUCKET_FOLDER",
        "DASK_GATEWAY_EOPF_ADDRESS",
        "DASK_CLUSTER_EOPF_NAME",
        "AWS_REQUEST_CHECKSUM_CALCULATION",
        "AWS_RESPONSE_CHECKSUM_VALIDATION",
        "TEMPO_ENDPOINT",
        "OTEL_PYTHON_REQUESTS_TRACE_HEADERS",
        "OTEL_PYTHON_REQUESTS_TRACE_BODY",
    ]
    if local_mode:
        keys.extend(
            [
                "LOCAL_DASK_USERNAME",
                "LOCAL_DASK_PASSWORD",
                "access_key",
                "bucket_location",
                "host_base",
                "host_bucket",
                "secret_key",
            ],
        )
    else:
        keys.extend(["JUPYTERHUB_API_TOKEN"])
    for key in keys:
        if value := caller_env.get(key):
            os.environ[key] = value


def dpr_tasktable_task(
    caller_env: dict[str, str],
    flow_span_context: SpanContext,
    use_mockup: bool,
    module_name: str,
    class_name: str,
):
    """
    Return the DPR tasktable. This function is run from inside the dask pod.
    """
    # Copy env vars from the caller
    copy_caller_env(caller_env)

    # Init opentelemetry and record all task in an Opentelemetry span
    init_opentelemetry.init_traces(None, SERVICE_NAME, logger)
    with init_opentelemetry.start_span(__name__, "main_dask_flow", flow_span_context):

        if use_mockup:
            time.sleep(1)
            return {}

        # Load the python class
        class_ = getattr(importlib.import_module(module_name), class_name)

        # Get the tasktable for default mode. See:
        # https://cpm.pages.eopf.copernicus.eu/eopf-cpm/main/processor-orchestration-guide/tasktables.html#tasktables
        logger.debug(f"Available modes for {class_}: {class_.get_available_modes()}")
        default_mode = class_.get_default_mode()
        tasktable = class_.get_tasktable_description(default_mode)
        return tasktable


def dpr_processor_task(  # pylint: disable=R0914, R0917
    caller_env: dict[str, str],
    data: dict,
    use_mockup: bool,
):
    """
    Run the DPR processor. This function is run from inside the dask pod.

    Args:
        caller_env: env variables coming from the caller
        data: data to send to the processor
        use_mockup: use the mockup or real processor
    """
    # Copy env vars from the caller
    copy_caller_env(caller_env)

    # Mockup processor
    if use_mockup:
        try:
            payload_abs_path = osp.join("/", os.getcwd(), "payload.cfg")
            with open(payload_abs_path, "w+", encoding="utf-8") as payload:
                payload.write(yaml.safe_dump(data))
        except Exception as e:
            logger.exception("Exception during payload file creation: %s", e)
            raise
        command = ["python3.11", "DPR_processor_mock.py", "-p", payload_abs_path]
        working_dir = "/src/DPR"
        log_path = "./mockup.log"  # not used
        logger.debug(f"Working directory for subprocess: {working_dir} (type: {type(working_dir)})")

    # Real processor
    else:
        # Read arguments
        s3_config_dir = data["s3_config_dir"]
        payload_subpath = data["payload_subpath"]
        s3_report_dir = data["s3_report_dir"]

        # Get S3 file handler.
        # NOTE: eopf exists in the dask worker environment, not in the rs-dpr-service env,
        # so we cannot import it from the top of this module.
        from eopf.common.file_utils import (  # pylint: disable=import-outside-toplevel
            AnyPath,
        )

        s3 = AnyPath(
            s3_config_dir,
            key=os.environ["S3_ACCESSKEY"],
            secret=os.environ["S3_SECRETKEY"],
            client_kwargs={
                "endpoint_url": os.environ["S3_ENDPOINT"],
                "region_name": os.environ["S3_REGION"],
            },
        )

        logger.info("The dpr processing task started")

        # Download the configuration folder from the S3 bucket into a local temp folder
        local_config_dir = s3.get(recursive=True)

        # Payload path and parent dir
        payload_file = osp.realpath(osp.join(local_config_dir, payload_subpath))
        payload_dir = osp.dirname(payload_file)

        with open(payload_file, encoding="utf-8") as opened:
            payload_contents = yaml.safe_load(opened)
            logger.debug(f"Payload file contents: {payload_file!r}\n{json.dumps(payload_contents, indent=2)}")

        command = ["eopf", "trigger", "local", payload_file]

        # Change working directory
        working_dir = osp.join(local_config_dir, payload_dir)
        os.chdir(working_dir)

        # Create the reports dir
        # WARNING: fields from the payload file: dask__export_graphs, performance_report_file, ... should
        # also use this directory: ./reports
        local_report_dir = osp.realpath("./reports")
        log_path = osp.join(local_report_dir, Path(payload_file).with_suffix(".processor.log").name)
        shutil.rmtree(local_report_dir, ignore_errors=True)
        os.makedirs(local_report_dir, exist_ok=True)

    # Trigger EOPF processing, catch output
    with subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=working_dir,
    ) as proc:

        # Log contents
        log_str = ""

        # Write output to a log file and string + redirect to the prefect logger
        with open(log_path, "w+", encoding="utf-8") as log_file:
            while proc.stdout and (line := proc.stdout.readline()) != "":

                # The log prints password in clear e.g 'key': '<my-secret>'... hide them with a regex
                for key in (
                    "key",
                    "secret",
                    "endpoint_url",
                    "region_name",
                    "api_token",
                    "password",
                ):
                    line = re.sub(rf"(\W{key}\W)[^,}}]*", r"\1: ***", line)

                # Write to log file and string
                log_file.write(line)
                log_str += line

                # Write to logger if not empty
                line = line.rstrip()
                if line:
                    logger.info(line)

        try:
            # Wait for the execution to finish
            status_code = proc.wait()

            # Raise exception if the status code is != 0
            if status_code:
                raise RuntimeError(f"EOPF error, status code {status_code!r}, please see the log.")
            logger.info(f"EOPF finished successfully with status code {status_code!r}")

            # search for the JSON-like part, parse it, and ignore the rest.
            if use_mockup:
                match = re.search(r"(\[\s*\{.*\}\s*\])", log_str, re.DOTALL)
                if not match:
                    raise ValueError(f"No valid dpr_payload structure found in the output:\n{log_str}")

                payload_str = match.group(1)

                # Use `ast.literal_eval` to safely evaluate the structure
                try:
                    # payload_str is a string that looks like a JSON, extracted from the dpr mockup's raw output.
                    # ast.literal_eval() parses that string and returns the actual Python object (not just the string).
                    return ast.literal_eval(payload_str)
                except Exception as e:
                    raise ValueError(f"Failed to parse dpr_payload structure: {e}") from e

            # NOTE: with the real processor, what should we return ?
            return {}

        # In all cases, upload the reports dir to the s3 bucket.
        finally:
            try:
                if not use_mockup:
                    logger.info(f"Upload reports {local_report_dir!r} to {s3_report_dir!r}")
                    s3._fs.put(local_report_dir, s3_report_dir, recursive=True)  # pylint: disable=protected-access
            except Exception as exception:  # pylint: disable=broad-exception-caught
                logger.error(exception)


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
