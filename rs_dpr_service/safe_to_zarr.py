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

Will run inside EOPF Dask cluster worker

The Sentinel-1 IW/EW SLC products are written in the format expected by the S1 ARD processor, which is NOT
compatible with the CPM 3.0.0 SLC model: see rs_dpr_service.s1_slc_s1ard_format.
"""

import contextlib
import importlib
import json
import os
import sys
import traceback
from collections.abc import Iterator

import eopf  # type: ignore
from eopf.store.convert import convert  # type: ignore


@contextlib.contextmanager
def s1ard_format_hook(safe_uri: str) -> Iterator[bool]:
    """
    Within this context, the EOPF conversion of a Sentinel-1 IW/EW SLC product writes it in the S1 ARD format.

    The EOPF convert() function reads the SAFE product with open_datatree, then writes the DataTree. We wrap
    open_datatree to transform the DataTree in between: lazily, without copying the measurement data.

    Yields:
        True if the product is a Sentinel-1 IW/EW SLC product, converted in the S1 ARD format.
    """
    # pylint: disable=import-outside-toplevel
    from rs_dpr_service import s1_slc_s1ard_format

    if not s1_slc_s1ard_format.is_s1_topsar_slc_safe(safe_uri):
        yield False
        return

    eopf_convert = importlib.import_module("eopf.store.convert")
    open_datatree = getattr(eopf_convert, "open_datatree")

    def open_in_s1ard_format(path, *args, **kwargs):
        if str(path).startswith("s3://"):
            raise ValueError(f"The SAFE product must be staged locally to be converted in the S1 ARD format: {path}")
        return s1_slc_s1ard_format.to_s1ard_format(open_datatree(path, *args, **kwargs), str(path))

    setattr(eopf_convert, "open_datatree", open_in_s1ard_format)
    try:
        yield True
    finally:
        setattr(eopf_convert, "open_datatree", open_datatree)


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
    # EOPF uploads to S3 with obstore, which only allows https endpoints by default
    if os.environ["S3_ENDPOINT"].startswith("http://"):
        os.environ["AWS_ALLOW_HTTP"] = "true"
    try:
        # stage_source/stage_target: download the SAFE and write the Zarr locally, then upload it to S3.
        with s1ard_format_hook(safe_uri) as s1ard_format:
            convert(
                safe_uri,
                zarr_uri,
                source_store_kwargs={"storage_options": s3_cfg},
                target_store_kwargs={"storage_options": s3_cfg, "zarr_format": cfg.get("zarr_format", 2)},
                stage_source=True,
                stage_target=True,
            )

        result_dict = {
            "message": "Conversion finished",
            "eopf_version": eopf.__version__,
            "safe_uri": safe_uri,
            "zarr_uri": zarr_uri,
        }
        if s1ard_format:
            # pylint: disable=import-outside-toplevel
            from rs_dpr_service.s1_slc_s1ard_format import S1ARD_FORMAT_PROPERTIES

            result_dict["product_format"] = S1ARD_FORMAT_PROPERTIES["rspy:product_format"]
            result_dict["warning"] = S1ARD_FORMAT_PROPERTIES["rspy:product_format_description"]
        result = json.dumps(result_dict)
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
