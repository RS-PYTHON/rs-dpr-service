# Copyright 2024 CS Group
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

"""Conversion Processor for converting a legacy product (safe format) into new Zarr format"""
import os
import uuid

import fsspec
from dask.distributed import Client
from pygeoapi.process.manager.postgresql import (
    PostgreSQLManager,  # pylint: disable=C0302
)
from pygeoapi.util import JobStatus
from starlette.requests import Request

from rs_dpr_service.call_dask import convert_safe_to_zarr
from rs_dpr_service.processors import GeneralProcessor


class ConversionProcessor(GeneralProcessor):
    """Runs an legacy product (safe format) conversion into new zarr format as a Dask job via subprocess."""

    def __init__(
        self,
        credentials: Request,
        db_process_manager: PostgreSQLManager,
    ):
        """
        Initialize Conversion Processor
        """
        super().__init__(credentials, db_process_manager, "ConversionProcessor")

    def _check_s3_config(self):
        """Validate the S3 bucket credentials."""
        try:
            s3_config = {
                "key": os.environ["S3_ACCESSKEY"],
                "secret": os.environ["S3_SECRETKEY"],
                "client_kwargs": {
                    "endpoint_url": os.environ["S3_ENDPOINT"],
                    "region_name": os.environ["S3_REGION"],
                },
            }
        except (KeyError, TypeError) as e:
            raise ValueError(f"Missing safe S3 config parameter: {e}") from e

        try:
            fs = fsspec.filesystem("s3", **s3_config)
            fs.ls("/")  # Minimal check to force auth
            return fs
        except Exception as e:
            raise ConnectionError(f"Failed to connect to safe S3: {e}") from e

    def _check_input_output_uris(self, s3_fs, data: dict):
        """Check that input legacy product exists and output bucket path exists."""

        safe_uri = data.get("input_safe_path", "")
        out_dir = data.get("output_zarr_dir_path", "").rstrip("/")
        if not safe_uri.startswith("s3://"):
            raise ValueError(f"Invalid input_safe_path format (must start with 's3://'): {safe_uri}")
        if not out_dir.startswith("s3://"):
            raise ValueError(f"Invalid output_zarr_dir_path format (must start with 's3://'): {out_dir}")

        path = safe_uri.replace("s3://", "")
        if not s3_fs.exists(path):
            raise FileNotFoundError(f"Input SAFE path does not exist: {safe_uri}")

        bucket = out_dir.replace("s3://", "").split("/", 1)[0]
        if not s3_fs.exists(bucket):
            raise FileNotFoundError(f"Output S3 bucket does not exist: {out_dir}")

    def _check_write_permission(self, fs, out_dir: str):
        """Check write permission on the output bucket."""
        bucket = out_dir.replace("s3://", "").split("/", 1)[0]
        test_key = f"{bucket}/.perm_test_{uuid.uuid4().hex}"
        try:
            with fs.open(test_key, "wb"):
                pass
            fs.rm(test_key, recursive=False)
        except Exception as e:
            if "AccessDenied" in str(e) or "UnauthorizedOperation" in str(e):
                raise PermissionError(f"No write permission on bucket: {out_dir}") from e
            raise RuntimeError(f"Error checking write permissions: {e}") from e

    async def execute(
        self,
        data: dict,
        outputs=None,
    ) -> tuple[str, dict]:
        """
        Asynchronously execute the conversion process.
        """
        try:
            s3_fs = self._check_s3_config()
            self._check_input_output_uris(s3_fs, data)
            self._check_write_permission(s3_fs, data["output_zarr_dir_path"])
        except Exception as e:  # pylint: disable=broad-exception-caught
            msg = str(e)
            self.logger.error(f"Conversion failed: {msg}")
            self.log_job_execution(JobStatus.failed, None, msg)
            return self._get_execute_result()

        # Start execution
        return await super().execute(data, outputs)

    def manage_dask_tasks(self, dask_client: Client | None, data: dict):
        """
        Schedule SAFE to Zarr conversion on the Dask cluster using a nested subprocess task.
        """
        if not dask_client:
            raise RuntimeError("Dask client is undefined")

        # Log start
        self.log_job_execution(JobStatus.running, 5, "Preparing conversion")
        try:
            # extract input parameter values
            safe_uri = data.get("input_safe_path")
            out_dir = data.get("output_zarr_dir_path", "").rstrip("/")
            basename = str(safe_uri).rsplit("/", 1)[-1].split(".", 1)[0]
            zarr_uri = f"{out_dir}/{basename}.zarr"

            # submit the task
            cfg = {
                "safe_uri": safe_uri,
                "zarr_uri": zarr_uri,
                "safe_s3_config": data.get("safe_s3_config", {}),
                "zarr_s3_config": data.get("zarr_s3_config", {}),
            }
            future = dask_client.submit(convert_safe_to_zarr, cfg)
            self.log_job_execution(JobStatus.running, 50, "Conversion job submitted to cluster")

            # wait for result
            res = future.result()
            self.log_job_execution(JobStatus.successful, 100, res)
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.logger.error(f"Conversion failed: {e}")
            self.log_job_execution(JobStatus.failed, None, f"Conversion failed: {e}")
        finally:
            dask_client.close()
