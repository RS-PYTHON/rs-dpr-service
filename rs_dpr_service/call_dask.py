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
import subprocess
import tempfile
import time
import zipfile
from pathlib import Path

import yaml
from distributed.client import Client as DaskClient
from opentelemetry.trace.span import SpanContext

from rs_dpr_service.utils import init_opentelemetry

SERVICE_NAME = "rs.dpr.dask"

local_mode: bool = os.getenv("RSPY_LOCAL_MODE") == "1"
cluster_mode: bool = not local_mode

logger = logging.getLogger(__name__)


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
        dask_client.upload_file(zip_path)


def copy_caller_env(caller_env: dict[str, str]):
    """
    Copy environment variables from the calling service environment to the dask client.

    Args:
        caller_env: os.environ coming from caller
    """
    # Update the local/clsuter mode global variable with the env var coming from the caller
    global local_mode, cluster_mode
    local_mode = caller_env.get("RSPY_LOCAL_MODE") == "1"
    cluster_mode = not local_mode

    # Copy env vars from the caller
    for key in [
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
    ] + (["LOCAL_DASK_USERNAME", "LOCAL_DASK_PASSWORD"] if local_mode else ["JUPYTERHUB_API_TOKEN"]):
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
    Dpr tasktable inside the dask cluster
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

        # Get the tasktable for default mode.
        # See: https://cpm.pages.eopf.copernicus.eu/eopf-cpm/main/processor-orchestration-guide/tasktables.html#tasktables
        logger.debug(f"Available modes for {class_}: {class_.get_available_modes()}")
        default_mode = class_.get_default_mode()
        tasktable = class_.get_tasktable_description(default_mode)
        return tasktable


def dpr_processor_task(  # pylint: disable=R0914, R0917
    caller_env: dict[str, str],
    dpr_payload: dict,
    use_mockup: bool,
):
    """
    Dpr processing inside the dask cluster
    """
    print("Dask task running - print() test")
    logger.info("The dpr processing task started")
    logger.info("Task started. Received dpr_payload = %s", json.dumps(dpr_payload, indent=2))
    try:
        payload_abs_path = osp.join("/", os.getcwd(), "payload.cfg")
        with open(payload_abs_path, "w+", encoding="utf-8") as payload:
            payload.write(yaml.safe_dump(dpr_payload))
    except Exception as e:
        logger.exception("Exception during payload file creation: %s", e)
        raise

    command = ["eopf", "trigger", "local", payload_abs_path]
    wd = "."
    if use_mockup:
        command = ["python3.11", "DPR_processor_mock.py", "-p", payload_abs_path]
        wd = "/src/DPR"
    logger.debug(f"Working directory for subprocess: {wd} (type: {type(wd)})")
    # Trigger EOPF processing, catch output
    assert isinstance(wd, str), f"Expected working directory (cwd) to be str, got {type(wd)}"
    with subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=wd,
    ) as p:
        assert p.stdout is not None  # For mypy
        # Log contents
        log_str = ""
        return_response = {}
        # Write output to a log file and string + redirect to the prefect logger
        with open(Path(payload_abs_path).with_suffix(".log").name, "w+", encoding="utf-8") as log_file:
            while (line := p.stdout.readline()) != "":

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

                # Write to prefect logger if not empty
                line = line.rstrip()
                if line:
                    logger.info(line)

            logger.info(f"log_str = {log_str}")
            # search for the JSON-like part, parse it, and ignore the rest.
            match = re.search(r"(\[\s*\{.*\}\s*\])", log_str, re.DOTALL)
            if not match:
                raise ValueError(f"No valid dpr_payload structure found in the output:\n{log_str}")

            payload_str = match.group(1)

            # Use `ast.literal_eval` to safely evaluate the structure
            try:
                # payload_str is a string that looks like a JSON, extracted from the dpr mockup's raw output.
                # ast.literal_eval() parses that string and returns the actual Python object (not just the string).
                return_response = ast.literal_eval(payload_str)
            except Exception as e:
                raise ValueError(f"Failed to parse dpr_payload structure: {e}") from e

        try:
            # Wait for the execution to finish
            status_code = p.wait()

            # Raise exception if the status code is != 0
            if status_code:
                raise Exception("EOPF error, please see the log.")  # pylint: disable=broad-exception-raised

        # In all cases, upload the reports dir to the s3 bucket.
        finally:
            time.sleep(1)

        return return_response
