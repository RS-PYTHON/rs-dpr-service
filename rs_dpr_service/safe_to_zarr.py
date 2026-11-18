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
import tempfile
import time
import traceback
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import eopf  # type: ignore
from eopf.store.convert import convert  # type: ignore

if TYPE_CHECKING:
    from obstore.store import ClientConfig, RetryConfig, S3Config

# Prefix of the S3 paths
S3_PREFIX = "s3://"

# Settings of the S3 clients of fsspec (botocore config) that upload the Zarr product.
# The object storage sometimes stops sending data in the middle of a request: wait longer than the 60 s default.
S3_CONFIG_KWARGS = {
    "connect_timeout": 60,
    "read_timeout": 300,
    "retries": {"max_attempts": 10, "mode": "adaptive"},
}

# Settings of the obstore client that downloads the SAFE product.
# read_timeout: maximum time without receiving data, to detect the stalled downloads (disabled by default).
# timeout: maximum time of a whole request, response body included (30 s by default): longer than the download
# of the biggest files of a SAFE product (~1.5 GiB for the measurement TIFF files of an IW SLC).
OBSTORE_CLIENT_OPTIONS: "ClientConfig" = {"connect_timeout": "60s", "read_timeout": "300s", "timeout": "2h"}
OBSTORE_RETRY_CONFIG: "RetryConfig" = {
    "max_retries": 10,
    "backoff": {"init_backoff": timedelta(seconds=1), "max_backoff": timedelta(seconds=60), "base": 2},
    "retry_timeout": timedelta(minutes=10),
}

# Number of files of the SAFE product downloaded at the same time
DOWNLOAD_CONCURRENCY = 4

# Minimum size of the chunks written in the local files
DOWNLOAD_CHUNK_SIZE = 16 * 1024 * 1024

# Download attempts of each file of the SAFE product, and delay in seconds before the first retry (doubled at each
# retry). A download interrupted in the middle of the response body is resumed from the bytes already downloaded.
DOWNLOAD_ATTEMPTS = 5
DOWNLOAD_RETRY_DELAY = 10


def is_transient_error(error: BaseException) -> bool:
    """Return True if the error, or one of its causes, is a network error that may not happen again."""
    # pylint: disable=import-outside-toplevel
    import obstore.exceptions

    # obstore raises a GenericError for the network errors (e.g. timeout, connection closed), and other
    # exceptions for the errors that would happen again (e.g. NotFoundError, PermissionDeniedError)
    transient_types = (TimeoutError, ConnectionError, obstore.exceptions.GenericError)
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        if isinstance(current, transient_types):
            return True
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return False


def create_obstore(bucket: str, storage_options: dict[str, Any]) -> Any:
    """Create the obstore S3 store of a bucket, from the fsspec S3 storage options."""
    # pylint: disable=import-outside-toplevel
    from obstore.store import S3Store

    client_kwargs = storage_options.get("client_kwargs") or {}
    # Same configuration as the EOPF storage_options_to_obstore_config function
    config: "S3Config" = {}
    if storage_options.get("key"):
        config["access_key_id"] = storage_options["key"]
    if storage_options.get("secret"):
        config["secret_access_key"] = storage_options["secret"]
    if storage_options.get("token"):
        config["session_token"] = storage_options["token"]
    if client_kwargs.get("endpoint_url"):
        config["endpoint"] = client_kwargs["endpoint_url"]
    if client_kwargs.get("region_name"):
        config["region"] = client_kwargs["region_name"]
    client_options: "ClientConfig" = {**OBSTORE_CLIENT_OPTIONS}
    if config.get("endpoint", "").startswith("http://"):
        client_options["allow_http"] = True
    return S3Store(bucket, config=config, client_options=client_options, retry_config=OBSTORE_RETRY_CONFIG)


def download_from_offset(store: Any, key: str, size: int, local_path: str) -> None:
    """
    Download a file of the object storage, from the bytes already written in the local file.

    Raise a ConnectionError if the file is not complete after the download.
    """
    # pylint: disable=import-outside-toplevel
    import obstore

    offset = os.path.getsize(local_path) if os.path.exists(local_path) else 0
    if offset > size:  # e.g. the file was modified in the object storage: download it again
        offset = 0
    if offset == size and os.path.exists(local_path):
        return
    options: Any = {"range": {"offset": offset}} if offset else None
    result = obstore.get(store, key, options=options)  # pylint: disable=no-member
    with open(local_path, "ab" if offset else "wb") as local_file:
        for chunk in result.stream(min_chunk_size=DOWNLOAD_CHUNK_SIZE):
            local_file.write(chunk)
    downloaded = os.path.getsize(local_path)
    if downloaded != size:
        raise ConnectionError(f"Incomplete download: {downloaded} bytes instead of {size}")


def download_file(store: Any, key: str, size: int, local_path: str) -> None:
    """
    Download a file of the object storage, with retries.

    After a network error, the download is resumed from the bytes already written in the local file.
    """
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        try:
            download_from_offset(store, key, size, local_path)
            return
        except Exception as error:  # pylint: disable=broad-exception-caught
            if attempt >= DOWNLOAD_ATTEMPTS or not is_transient_error(error):
                raise
            delay = DOWNLOAD_RETRY_DELAY * 2 ** (attempt - 1)
            print(
                f"Failed to download {key!r} (attempt {attempt}/{DOWNLOAD_ATTEMPTS}): "
                f"{type(error).__name__}: {error}. Retry in {delay} s.",
                file=sys.stderr,
            )
            time.sleep(delay)


def stage_safe(safe_uri: str, local_dir: str, storage_options: dict[str, Any]) -> str:
    """
    Download the SAFE product from S3 into a local directory with obstore, and return its local path.

    EOPF downloads the SAFE product with fsspec in a single call, without retry: a network error on any file of the
    product (~10 GiB for an IW SLC) fails the whole conversion. Here several files are downloaded at the same time,
    and each file is retried on its own, from the bytes already downloaded.

    Args:
        safe_uri: s3://<bucket>/<path> of the SAFE product directory, or of a single file (e.g. a zip archive)
        local_dir: local directory where the SAFE product is downloaded
        storage_options: fsspec S3 storage options
    """
    # pylint: disable=import-outside-toplevel
    import obstore

    bucket, _, root_key = safe_uri.removeprefix(S3_PREFIX).rstrip("/").partition("/")
    store = create_obstore(bucket, storage_options)
    local_safe = os.path.join(local_dir, os.path.basename(root_key))

    # Files of the SAFE product directory: path in the bucket, size and local path
    files = [
        (meta["path"], meta["size"], os.path.join(local_safe, os.path.relpath(meta["path"], root_key)))
        for batch in obstore.list(store, prefix=f"{root_key}/")  # pylint: disable=no-member
        for meta in batch
    ]
    if not files:
        # Not a directory: a single file, or raise a NotFoundError
        files = [(root_key, obstore.head(store, root_key)["size"], local_safe)]  # pylint: disable=no-member

    total_size = sum(size for _, size, _ in files)
    print(
        f"Download {len(files)} files ({total_size / 2**30:.2f} GiB) of {safe_uri!r} to {local_safe!r}",
        file=sys.stderr,
    )
    started_at = time.time()
    with ThreadPoolExecutor(max_workers=DOWNLOAD_CONCURRENCY) as executor:
        futures = [executor.submit(download_file, store, *file) for file in files]
        try:
            for future in futures:
                future.result()
        except BaseException:
            # Don't start the other downloads: the conversion fails
            for future in futures:
                future.cancel()
            raise
    elapsed = max(time.time() - started_at, 1e-3)
    print(
        f"Downloaded {safe_uri!r} in {elapsed:.1f} s ({total_size / 2**20 / elapsed:.1f} MiB/s)",
        file=sys.stderr,
    )
    return local_safe


@contextlib.contextmanager
def retrying_source_staging() -> Iterator[None]:
    """
    Within this context, the EOPF conversion downloads the S3 source product with stage_safe, that retries the
    network errors, instead of the EOPF _stage_source function. The local copy is removed at the end of the context.

    We wrap the EOPF function instead of giving the local copy to convert(), so that EOPF still writes the S3 path of
    the source product, not its local path, in the processing history of the converted product.
    """
    eopf_convert = importlib.import_module("eopf.store.convert")
    eopf_stage_source = getattr(eopf_convert, "_stage_source", None)
    if eopf_stage_source is None:
        # Private EOPF function, that may change in another EOPF version
        print("eopf.store.convert._stage_source not found: the source product is staged by EOPF", file=sys.stderr)
        yield
        return

    with tempfile.TemporaryDirectory(prefix="rspy-safe-") as staging_dir:

        def stage_source(source_path, source_store_kwargs, stage, logger):
            if not stage or not str(source_path).startswith(S3_PREFIX):
                return eopf_stage_source(source_path, source_store_kwargs, stage, logger)
            storage_options = source_store_kwargs.get("storage_options") or {}
            return stage_safe(str(source_path), staging_dir, storage_options), contextlib.nullcontext()

        setattr(eopf_convert, "_stage_source", stage_source)
        try:
            yield
        finally:
            setattr(eopf_convert, "_stage_source", eopf_stage_source)


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
        if str(path).startswith(S3_PREFIX):
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
        "config_kwargs": S3_CONFIG_KWARGS,
    }
    # EOPF uploads to S3 with obstore, which only allows https endpoints by default
    if os.environ["S3_ENDPOINT"].startswith("http://"):
        os.environ["AWS_ALLOW_HTTP"] = "true"
    try:
        # stage_source/stage_target: download the SAFE (with retries) and write the Zarr locally, then upload it to S3.
        with retrying_source_staging(), s1ard_format_hook(safe_uri) as s1ard_format:
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
