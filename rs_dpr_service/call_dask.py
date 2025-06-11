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


def upload_this_module(dask_client: DaskClient):
    """
    Upload this current module from the caller environment to the dask client.

    WARNING: These modules should not import other modules that are not installed in the dask
    environment or you'll have import errors.

    Args:
        clients: list of dask clients to which upload the modules.
    """

    this_module = Path(__file__).absolute()
    this_init = this_module.parent / "__init__.py"
    this_project = this_module.parent.parent

    # Files to upload and associated name in the zip archive
    files = {
        this_init: this_init.relative_to(this_project),
        this_module: this_module.relative_to(this_project),
    }

    # From a temp dir
    with tempfile.TemporaryDirectory() as tmpdir:

        # Create a zip with our files
        zip_path = f"{tmpdir}/{this_module.parent.name}.zip"
        with zipfile.ZipFile(zip_path, "w") as zipped:

            # Zip all files
            for key, value in files.items():
                zipped.write(str(key), str(value))

        # Upload zip file to dask clients.
        # This also installs the zipped modules inside the dask python interpreter.
        dask_client.upload_file(zip_path)


def dpr_processor_task(  # pylint: disable=R0914, R0917
    dpr_payload: dict,
):
    """
    Dpr processing inside the dask cluster
    """

    logger = logging.getLogger(__name__)

    print("Dask task running - print() test")
    logger.info("The dpr processing task started")
    logger.info("Task started. Received dpr_payload = %s", json.dumps(dpr_payload, indent=2))
    use_mockup = dpr_payload.get("use_mockup", False)
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
