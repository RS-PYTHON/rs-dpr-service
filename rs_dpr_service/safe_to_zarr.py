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
Convert a legacy product (safe format) product to Zarr format using EOPF.

References:
- EOPF documentation: https://cpm.pages.eopf.copernicus.eu/eopf-cpm/main/index.html

Will run inside EOPF Dask cluster worker, with EOPF CPM 3.x (cpm3 cluster) or 2.x (cpm2 cluster).
"""

import json
import os
import sys
import traceback

import eopf  # type: ignore
from eopf.store.convert import convert  # type: ignore


def convert_cpm2(safe_uri: str, zarr_uri: str, s3_cfg: dict):
    """Convert with EOPF CPM 2.x, that downloads the SAFE locally, writes the Zarr to S3, and only writes Zarr v2."""
    # pylint: disable=import-outside-toplevel
    from eopf.common.file_utils import AnyPath  # type: ignore
    from eopf.config import EOConfiguration  # type: ignore

    # Don't create a Dask cluster in the conversion subprocess
    EOConfiguration()["store__convert__use_multithreading"] = False

    # CPM 2.x requires an existing output "folder": in S3, a folder only exists if it contains an object, so create a
    # marker object like CPM 2.x does (AnyPath.mkdir), and remove it after the conversion
    zarr = AnyPath(zarr_uri, **s3_cfg)
    output_dir = zarr.dirname()
    keep_file = None
    if not output_dir.exists():
        keep_file = output_dir / ".keep"
        keep_file.touch()
    try:
        convert(AnyPath(safe_uri, **s3_cfg), zarr)
    finally:
        if keep_file is not None:
            keep_file.rm()


def convert_cpm3(safe_uri: str, zarr_uri: str, s3_cfg: dict, zarr_format: int):
    """Convert with EOPF CPM 3.x"""
    # EOPF uploads to S3 with obstore, which only allows https endpoints by default
    if os.environ["S3_ENDPOINT"].startswith("http://"):
        os.environ["AWS_ALLOW_HTTP"] = "true"
    # stage_source/stage_target: download the SAFE and write the Zarr locally, then upload it to S3.
    convert(
        safe_uri,
        zarr_uri,
        source_store_kwargs={"storage_options": s3_cfg},
        target_store_kwargs={"storage_options": s3_cfg, "zarr_format": zarr_format},
        stage_source=True,
        stage_target=True,
    )


def main():
    """
    Convert from legacy product (safe format) into Zarr format using EOPF in a subprocess.

    Usage: python -m rs_dpr_service.safe_to_zarr '<json_string>' [<result_fd>]

    The conversion result is printed, and also written into the inherited file descriptor <result_fd>
    (the write end of a pipe) if given, so the caller can read it without parsing the logs.
    """
    if len(sys.argv) < 2:
        print("Usage: python safe_to_zarr.py '<json_string>' [<result_fd>]", file=sys.stderr)
        sys.exit(1)

    try:
        cfg = json.loads(sys.argv[1])
    except json.JSONDecodeError as e:
        print(f"Failed to decode config JSON: {e}", file=sys.stderr)
        sys.exit(1)

    # Converting a legacy product stored in a s3 bucket (safe format) into new Zarr format
    safe_uri = cfg["safe_uri"]
    zarr_uri = cfg["zarr_uri"]
    s3_cfg = {
        "key": os.environ["S3_ACCESSKEY"],
        "secret": os.environ["S3_SECRETKEY"],
        "client_kwargs": {
            "endpoint_url": os.environ["S3_ENDPOINT"],
            "region_name": os.environ["S3_REGION"],
        },
    }
    try:
        if int(eopf.__version__.split(".", 1)[0]) < 3:
            convert_cpm2(safe_uri, zarr_uri, s3_cfg)
        else:
            convert_cpm3(safe_uri, zarr_uri, s3_cfg, cfg.get("zarr_format", 2))

        result = json.dumps(
            {
                "message": "Conversion finished",
                "eopf_version": eopf.__version__,
                "safe_uri": safe_uri,
                "zarr_uri": zarr_uri,
            },
        )
        print(result)
        if len(sys.argv) > 2:
            with os.fdopen(int(sys.argv[2]), "w", encoding="utf-8") as result_file:
                result_file.write(result)
    except Exception as e:  # pylint: disable=broad-exception-caught
        traceback.print_exc()
        print(f"Conversion failed safe_to_zarr: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
