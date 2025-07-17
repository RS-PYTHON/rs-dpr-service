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
Convert a legacy product (safe format) product to Zarr format using EOPF.

References:
- EOPF documentation: https://cpm.pages.eopf.copernicus.eu/eopf-cpm/main/index.html

Will run inside EOPF Dask cluster worker
"""
import json
import os
import sys

import eopf  # type: ignore
from eopf.common.file_utils import AnyPath  # type: ignore
from eopf.config import EOConfiguration  # type: ignore
from eopf.store.convert import convert  # type: ignore


def main():
    """Convert from legacy product (safe format) into Zarr format using EOPF in a subprocess."""
    if len(sys.argv) < 2:
        print("Usage: python safe_to_zarr.py '<json_string>'", file=sys.stderr)
        sys.exit(1)

    try:
        cfg = json.loads(sys.argv[1])
    except json.JSONDecodeError as e:
        print(f"Failed to decode config JSON: {e}", file=sys.stderr)
        sys.exit(1)

    # do not use dask cluster
    EOConfiguration()["store__convert__use_multithreading"] = False

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
        safe = AnyPath(safe_uri, **s3_cfg)
        zarr = AnyPath(zarr_uri, **s3_cfg)
        convert(safe, zarr)

        print(
            json.dumps(
                {
                    "message": "Conversion finished",
                    "eopf_version": eopf.__version__,
                    "safe_uri": safe_uri,
                    "zarr_uri": zarr_uri,
                },
            ),
        )
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Conversion failed safe_to_zarr: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
