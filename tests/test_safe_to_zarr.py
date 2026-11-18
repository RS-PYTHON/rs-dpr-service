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

"""Tests for the rs_dpr_service.safe_to_zarr conversion entrypoint."""

import importlib
import inspect
import json
import os
import runpy
import sys
import threading
import types
from contextlib import redirect_stderr, redirect_stdout
from datetime import timedelta
from io import StringIO
from pathlib import Path

import obstore.exceptions
import obstore.store
import pytest


def install_fake_eopf_dependencies(monkeypatch, mocker):
    """Install mocked EOPF dependencies in sys.modules."""
    eo_config: dict[str, bool] = {}
    any_path = mocker.Mock(side_effect=lambda path, **kwargs: {"path": path, "kwargs": kwargs})
    convert = mocker.Mock()

    # safe_to_zarr imports these modules at import time, so they must exist before run_module/import_module.
    fake_eopf = types.SimpleNamespace(__version__="1.2.3")
    fake_common = types.ModuleType("eopf.common")
    # Keep the two observable EOPF calls as mocks: path construction and conversion execution.
    fake_file_utils = types.SimpleNamespace(AnyPath=any_path)
    fake_config = types.SimpleNamespace(EOConfiguration=lambda: eo_config)
    fake_store = types.ModuleType("eopf.store")
    # open_datatree is wrapped by safe_to_zarr to convert the Sentinel-1 SLC products in the S1 ARD format
    # _stage_source is wrapped by safe_to_zarr to download the source product with retries
    fake_convert = types.SimpleNamespace(
        convert=convert,
        open_datatree=mocker.Mock(name="open_datatree"),
        _stage_source=mocker.Mock(name="_stage_source", return_value=("/eopf/staging/input.SAFE", None)),
    )

    # Populate the full import tree used by rs_dpr_service.safe_to_zarr.
    for name, module in {
        "eopf": fake_eopf,
        "eopf.common": fake_common,
        "eopf.common.file_utils": fake_file_utils,
        "eopf.config": fake_config,
        "eopf.store": fake_store,
        "eopf.store.convert": fake_convert,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

    return eo_config, any_path, convert


def import_safe_to_zarr(monkeypatch, mocker):
    """Import the module with mocked EOPF dependencies."""
    eo_config, any_path, convert = install_fake_eopf_dependencies(monkeypatch, mocker)

    monkeypatch.delitem(sys.modules, "rs_dpr_service.safe_to_zarr", raising=False)
    module = importlib.import_module("rs_dpr_service.safe_to_zarr")
    return module, eo_config, any_path, convert


def module_retry_config():
    """Expected obstore retry config."""
    return {
        "max_retries": 10,
        "backoff": {"init_backoff": timedelta(seconds=1), "max_backoff": timedelta(seconds=60), "base": 2},
        "retry_timeout": timedelta(minutes=10),
    }


def run_safe_to_zarr_as_main(monkeypatch, argv):
    """Run rs_dpr_service.safe_to_zarr as a script."""
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.delitem(sys.modules, "rs_dpr_service.safe_to_zarr", raising=False)
    return runpy.run_module("rs_dpr_service.safe_to_zarr", run_name="__main__")


class FakeGetResult:
    """Fake obstore GetResult: stream the content, or raise an error after some bytes."""

    def __init__(self, content: bytes, error: tuple[int, Exception] | None):
        self.content = content
        self.error = error

    def stream(self, min_chunk_size):
        """Return the chunks of the content."""
        assert min_chunk_size > 0
        if self.error:
            written, exception = self.error
            yield self.content[:written]
            raise exception
        yield self.content


class FakeObstore:
    """Fake obstore functions, on the files of a bucket, with errors to simulate."""

    def __init__(self, files: dict[str, bytes]):
        self.files = files
        self.store_kwargs: dict = {}
        # Errors of the next get calls of a file: exception raised by get(), or (bytes, exception) raised by the
        # stream after these bytes
        self.errors: dict[str, list] = {}
        # (path, offset) of each get call
        self.get_calls: list[tuple[str, int]] = []
        self.lock = threading.Lock()

    def s3_store(self, bucket, **kwargs):
        """Create the store: return the bucket name."""
        self.store_kwargs = {"bucket": bucket, **kwargs}
        return bucket

    def list(self, store, prefix):
        """List the files under a prefix, by batches."""
        assert store == "bucket"
        return [
            [{"path": path, "size": len(content)} for path, content in self.files.items() if path.startswith(prefix)],
        ]

    def head(self, store, path):
        """Return the metadata of a file."""
        assert store == "bucket"
        if path not in self.files:
            raise obstore.exceptions.NotFoundError(f"Not found: {path}")
        return {"path": path, "size": len(self.files[path])}

    def get(self, store, path, options=None):
        """Return the content of a file, from an offset, or raise the next error."""
        assert store == "bucket"
        offset = options["range"]["offset"] if options else 0
        with self.lock:
            self.get_calls.append((path, offset))
            error = self.errors.get(path, []).pop(0) if self.errors.get(path) else None
        if isinstance(error, Exception):
            raise error
        return FakeGetResult(self.files[path][offset:], error)


SAFE_FILES = {
    "input.SAFE/manifest.safe": b"<manifest/>",
    "input.SAFE/measurement/s1a-iw1-slc-vv.tiff": b"tiff data of the measurement",
}


@pytest.fixture(name="fake_obstore")
def fake_obstore_fixture(monkeypatch):
    """Replace the obstore functions by fakes, and don't wait before the download retries."""
    fake = FakeObstore(dict(SAFE_FILES))
    monkeypatch.setattr("obstore.store.S3Store", fake.s3_store)
    monkeypatch.setattr("obstore.list", fake.list)
    monkeypatch.setattr("obstore.head", fake.head)
    monkeypatch.setattr("obstore.get", fake.get)
    monkeypatch.setattr("time.sleep", lambda _: None)
    return fake


def test_main_without_args_exits(monkeypatch, mocker):
    """Exit with usage message when JSON config is missing."""
    # Import-time EOPF dependencies are stubbed so the script can be exercised in isolation.
    install_fake_eopf_dependencies(monkeypatch, mocker)

    stdout = StringIO()
    stderr = StringIO()
    # Missing config is reported as CLI usage on stderr and exits before conversion starts.
    with redirect_stdout(stdout), redirect_stderr(stderr), pytest.raises(SystemExit) as exc:
        run_safe_to_zarr_as_main(monkeypatch, ["safe_to_zarr.py"])

    assert exc.value.code == 1
    assert stdout.getvalue() == ""
    assert "Usage: python safe_to_zarr.py" in stderr.getvalue()


def test_main_with_invalid_json_exits(monkeypatch, mocker):
    """Exit with an error when the JSON payload cannot be decoded."""
    # The script imports EOPF before validating argv, so keep those imports mocked here too.
    install_fake_eopf_dependencies(monkeypatch, mocker)

    stdout = StringIO()
    stderr = StringIO()
    # Invalid JSON should fail before any conversion call is attempted.
    with redirect_stdout(stdout), redirect_stderr(stderr), pytest.raises(SystemExit) as exc:
        run_safe_to_zarr_as_main(monkeypatch, ["safe_to_zarr.py", "{not-json}"])

    assert exc.value.code == 1
    assert stdout.getvalue() == ""
    assert "Failed to decode config JSON" in stderr.getvalue()


def test_main_success(monkeypatch, mocker, fake_obstore):
    """Download the SAFE product, convert it to Zarr and print the success payload."""
    # Observe the conversion call without importing real EOPF.
    _, _, convert = install_fake_eopf_dependencies(monkeypatch, mocker)
    eopf_convert = sys.modules["eopf.store.convert"]
    eopf_stage_source = eopf_convert._stage_source  # pylint: disable=protected-access
    cfg = {"safe_uri": "s3://bucket/input.SAFE", "zarr_uri": "s3://bucket/output.zarr"}
    staged = {}

    def fake_convert(source_path, _target_path, source_store_kwargs, **_kwargs):
        # convert() stages the source product with the wrapped _stage_source
        local_path, _ = eopf_convert._stage_source(  # pylint: disable=protected-access
            source_path,
            source_store_kwargs,
            True,
            None,
        )
        staged["path"] = local_path
        staged["files"] = sorted(str(path.relative_to(local_path)) for path in Path(local_path).rglob("*.*"))

    convert.side_effect = fake_convert
    monkeypatch.setenv("S3_ACCESSKEY", "access")
    monkeypatch.setenv("S3_SECRETKEY", "secret")
    monkeypatch.setenv("S3_ENDPOINT", "https://example.com")
    monkeypatch.setenv("S3_REGION", "eu-west-1")

    stdout = StringIO()
    stderr = StringIO()
    # Successful execution writes only the JSON result to stdout.
    with redirect_stdout(stdout), redirect_stderr(stderr):
        run_safe_to_zarr_as_main(monkeypatch, ["safe_to_zarr.py", json.dumps(cfg)])

    assert "Conversion failed" not in stderr.getvalue()

    expected_s3_cfg = {
        "key": "access",
        "secret": "secret",  # nosec
        "client_kwargs": {
            "endpoint_url": "https://example.com",
            "region_name": "eu-west-1",
        },
        "config_kwargs": {
            "connect_timeout": 60,
            "read_timeout": 300,
            "retries": {"max_attempts": 10, "mode": "adaptive"},
        },
    }
    convert.assert_called_once_with(
        cfg["safe_uri"],
        cfg["zarr_uri"],
        source_store_kwargs={"storage_options": expected_s3_cfg},
        target_store_kwargs={"storage_options": expected_s3_cfg, "zarr_format": 2},
        stage_source=True,
        stage_target=True,
    )
    # The SAFE product was downloaded by safe_to_zarr, with the S3 settings, instead of EOPF
    eopf_stage_source.assert_not_called()
    assert fake_obstore.store_kwargs == {
        "bucket": "bucket",
        "config": {
            "access_key_id": "access",
            "secret_access_key": "secret",  # nosec
            "endpoint": "https://example.com",
            "region": "eu-west-1",
        },
        "client_options": {"connect_timeout": "60s", "read_timeout": "300s", "timeout": "2h"},
        "retry_config": module_retry_config(),
    }
    assert staged["path"].endswith(os.sep + "input.SAFE")
    assert staged["files"] == ["manifest.safe", os.path.join("measurement", "s1a-iw1-slc-vv.tiff")]
    # The local copy is removed after the conversion, and the EOPF function is restored
    assert not os.path.exists(staged["path"])
    assert eopf_convert._stage_source is eopf_stage_source  # pylint: disable=protected-access

    result = json.loads(stdout.getvalue())
    assert result == {
        "message": "Conversion finished",
        "eopf_version": "1.2.3",
        "safe_uri": cfg["safe_uri"],
        "zarr_uri": cfg["zarr_uri"],
    }


def test_main_writes_the_result_pipe(monkeypatch, mocker, fake_obstore):  # pylint: disable=unused-argument
    """Write the success payload into the inherited file descriptor given as second argument."""
    install_fake_eopf_dependencies(monkeypatch, mocker)
    cfg = {"safe_uri": "s3://bucket/input.SAFE", "zarr_uri": "s3://bucket/output.zarr"}
    for key, value in {
        "S3_ACCESSKEY": "access",
        "S3_SECRETKEY": "secret",
        "S3_ENDPOINT": "https://example.com",
        "S3_REGION": "eu-west-1",
    }.items():
        monkeypatch.setenv(key, value)
    result_read, result_write = os.pipe()

    with redirect_stdout(StringIO()):
        run_safe_to_zarr_as_main(monkeypatch, ["safe_to_zarr.py", json.dumps(cfg), str(result_write)])

    # main() closed the write end, so the read returns at the end of the result
    with os.fdopen(result_read, encoding="utf-8") as result_file:
        assert json.loads(result_file.read())["message"] == "Conversion finished"


def test_main_conversion_failure_exits(monkeypatch, mocker, fake_obstore):  # pylint: disable=unused-argument
    """Exit with an error when the conversion raises an exception."""
    # Force only the conversion step to fail; argument parsing and path setup stay real.
    _, _, convert = install_fake_eopf_dependencies(monkeypatch, mocker)
    cfg = {"safe_uri": "s3://bucket/input.SAFE", "zarr_uri": "s3://bucket/output.zarr"}
    monkeypatch.setenv("S3_ACCESSKEY", "access")
    monkeypatch.setenv("S3_SECRETKEY", "secret")
    monkeypatch.setenv("S3_ENDPOINT", "https://example.com")
    monkeypatch.setenv("S3_REGION", "eu-west-1")
    convert.side_effect = RuntimeError("boom")

    stdout = StringIO()
    stderr = StringIO()
    # Conversion errors are reported on stderr and translated into process exit code 1.
    with redirect_stdout(stdout), redirect_stderr(stderr), pytest.raises(SystemExit) as exc:
        run_safe_to_zarr_as_main(monkeypatch, ["safe_to_zarr.py", json.dumps(cfg)])

    assert exc.value.code == 1
    assert stdout.getvalue() == ""
    assert "Conversion failed safe_to_zarr: boom" in stderr.getvalue()
    # The traceback is printed too, to find the root cause
    assert "Traceback (most recent call last)" in stderr.getvalue()


S1_SLC_SAFE = "s3://bucket/S1A_IW_SLC__1SDV_20260421T234638_20260421T234646_064184_08142A_7054.SAFE"


def set_s3_env(monkeypatch):
    """Set the S3 env vars read by safe_to_zarr."""
    for key, value in {
        "S3_ACCESSKEY": "access",
        "S3_SECRETKEY": "secret",
        "S3_ENDPOINT": "https://example.com",
        "S3_REGION": "eu-west-1",
    }.items():
        monkeypatch.setenv(key, value)


def test_main_converts_the_s1_slc_products_in_the_s1ard_format(monkeypatch, mocker):
    """The Sentinel-1 SLC products are transformed into the S1 ARD format between their reading and writing."""
    _, _, convert = install_fake_eopf_dependencies(monkeypatch, mocker)
    eopf_convert = sys.modules["eopf.store.convert"]
    original_open_datatree = eopf_convert.open_datatree
    original_open_datatree.return_value = "CPM 3.0.0 product"
    to_s1ard_format = mocker.patch(
        "rs_dpr_service.s1_slc_s1ard_format.to_s1ard_format",
        return_value="S1 ARD product",
    )

    def fake_convert(safe_uri, *_args, **_kwargs):
        # convert() reads the staged SAFE product with the wrapped open_datatree
        assert eopf_convert.open_datatree is not original_open_datatree
        assert eopf_convert.open_datatree("/staging/product.SAFE", engine="safe") == "S1 ARD product"
        original_open_datatree.assert_called_once_with("/staging/product.SAFE", engine="safe")
        to_s1ard_format.assert_called_once_with("CPM 3.0.0 product", "/staging/product.SAFE")
        assert safe_uri == S1_SLC_SAFE

    convert.side_effect = fake_convert
    set_s3_env(monkeypatch)
    cfg = {"safe_uri": S1_SLC_SAFE, "zarr_uri": "s3://bucket/output.zarr"}

    stdout = StringIO()
    with redirect_stdout(stdout), redirect_stderr(StringIO()):
        run_safe_to_zarr_as_main(monkeypatch, ["safe_to_zarr.py", json.dumps(cfg)])

    convert.assert_called_once()
    # The original EOPF function is restored after the conversion
    assert eopf_convert.open_datatree is original_open_datatree
    result = json.loads(stdout.getvalue())
    assert result["product_format"] == "s1ard-1.2.0"
    assert "NOT compatible with the CPM 3.0.0 SLC model" in result["warning"]
    assert "https://gitlab.eopf.copernicus.eu/cpm/eopf-cpm/-/issues/1042" in result["warning"]


def test_s1ard_format_hook_needs_a_staged_safe_product(monkeypatch, mocker):
    """The SAFE XML files are read locally: the SAFE product must be staged."""
    module, _, _, _ = import_safe_to_zarr(monkeypatch, mocker)
    eopf_convert = sys.modules["eopf.store.convert"]

    with module.s1ard_format_hook(S1_SLC_SAFE) as s1ard_format:
        assert s1ard_format is True
        with pytest.raises(ValueError, match="must be staged locally"):
            eopf_convert.open_datatree(S1_SLC_SAFE)


def test_s1ard_format_hook_ignores_the_other_products(monkeypatch, mocker):
    """The other products are converted by EOPF as they are."""
    module, _, _, _ = import_safe_to_zarr(monkeypatch, mocker)
    eopf_convert = sys.modules["eopf.store.convert"]
    original_open_datatree = eopf_convert.open_datatree

    with module.s1ard_format_hook("s3://bucket/S1A_IW_GRDH_1SDV_20260421T234638.SAFE") as s1ard_format:
        assert s1ard_format is False
        assert eopf_convert.open_datatree is original_open_datatree


def test_stage_safe_resumes_the_interrupted_downloads(monkeypatch, mocker, fake_obstore, tmp_path):
    """A download interrupted by a network error is resumed from the bytes already downloaded."""
    module, _, _, _ = import_safe_to_zarr(monkeypatch, mocker)
    tiff = "input.SAFE/measurement/s1a-iw1-slc-vv.tiff"
    fake_obstore.errors[tiff] = [
        # Read timeout after 4 bytes, error before the response, then read timeout after 6 more bytes
        (4, obstore.exceptions.GenericError("Generic S3 error: error decoding response body: operation timed out")),
        ConnectionResetError("reset"),
        (6, TimeoutError()),
    ]

    with redirect_stderr(StringIO()):
        local_safe = module.stage_safe("s3://bucket/input.SAFE/", str(tmp_path), {"key": "access"})

    assert local_safe == str(tmp_path / "input.SAFE")
    for path, content in SAFE_FILES.items():
        assert (tmp_path / path).read_bytes() == content
    assert sorted(call for call in fake_obstore.get_calls if call[0] == tiff) == [
        (tiff, 0),
        (tiff, 4),
        (tiff, 4),
        (tiff, 10),
    ]
    assert fake_obstore.get_calls.count(("input.SAFE/manifest.safe", 0)) == 1


def test_stage_safe_skips_the_files_already_downloaded(monkeypatch, mocker, fake_obstore, tmp_path):
    """The files already downloaded are not downloaded again, and a file bigger than expected is downloaded again."""
    module, _, _, _ = import_safe_to_zarr(monkeypatch, mocker)
    (tmp_path / "input.SAFE" / "measurement").mkdir(parents=True)
    (tmp_path / "input.SAFE" / "manifest.safe").write_bytes(SAFE_FILES["input.SAFE/manifest.safe"])
    (tmp_path / "input.SAFE" / "measurement" / "s1a-iw1-slc-vv.tiff").write_bytes(b"x" * 100)

    with redirect_stderr(StringIO()):
        module.stage_safe("s3://bucket/input.SAFE", str(tmp_path), {})

    assert fake_obstore.get_calls == [("input.SAFE/measurement/s1a-iw1-slc-vv.tiff", 0)]
    for path, content in SAFE_FILES.items():
        assert (tmp_path / path).read_bytes() == content


def test_stage_safe_gives_up_after_the_max_attempts(monkeypatch, mocker, fake_obstore, tmp_path):
    """The download fails after DOWNLOAD_ATTEMPTS network errors on the same file."""
    module, _, _, _ = import_safe_to_zarr(monkeypatch, mocker)
    fake_obstore.errors["input.SAFE/manifest.safe"] = [ConnectionResetError("reset")] * module.DOWNLOAD_ATTEMPTS

    with pytest.raises(ConnectionResetError), redirect_stderr(StringIO()):
        module.stage_safe("s3://bucket/input.SAFE", str(tmp_path), {})
    assert fake_obstore.get_calls.count(("input.SAFE/manifest.safe", 0)) == module.DOWNLOAD_ATTEMPTS


def test_stage_safe_doesnt_retry_the_other_errors(monkeypatch, mocker, fake_obstore, tmp_path):
    """An error that is not a network error, e.g. an access denied, is raised at once."""
    module, _, _, _ = import_safe_to_zarr(monkeypatch, mocker)
    fake_obstore.errors["input.SAFE/manifest.safe"] = [obstore.exceptions.PermissionDeniedError("Access Denied")]

    with pytest.raises(obstore.exceptions.PermissionDeniedError), redirect_stderr(StringIO()):
        module.stage_safe("s3://bucket/input.SAFE", str(tmp_path), {})
    assert fake_obstore.get_calls.count(("input.SAFE/manifest.safe", 0)) == 1


def test_stage_safe_downloads_a_single_file(monkeypatch, mocker, fake_obstore, tmp_path):
    """A SAFE product stored as a single file (e.g. a zip archive) is downloaded as a file."""
    module, _, _, _ = import_safe_to_zarr(monkeypatch, mocker)
    fake_obstore.files = {"input.SAFE.zip": b"zip data", "input.SAFE.zip.md5": b"md5"}

    with redirect_stderr(StringIO()):
        local_safe = module.stage_safe("s3://bucket/input.SAFE.zip", str(tmp_path), {})

    assert local_safe == str(tmp_path / "input.SAFE.zip")
    assert (tmp_path / "input.SAFE.zip").read_bytes() == b"zip data"
    assert fake_obstore.get_calls == [("input.SAFE.zip", 0)]


def test_stage_safe_downloads_the_empty_files(monkeypatch, mocker, fake_obstore, tmp_path):
    """The empty files are created."""
    module, _, _, _ = import_safe_to_zarr(monkeypatch, mocker)
    fake_obstore.files = {"input.SAFE/empty.txt": b""}

    with redirect_stderr(StringIO()):
        module.stage_safe("s3://bucket/input.SAFE", str(tmp_path), {})

    assert (tmp_path / "input.SAFE" / "empty.txt").read_bytes() == b""


def test_stage_safe_fails_without_files(monkeypatch, mocker, fake_obstore, tmp_path):
    """A missing SAFE product is reported."""
    module, _, _, _ = import_safe_to_zarr(monkeypatch, mocker)
    fake_obstore.files = {}

    with pytest.raises(obstore.exceptions.NotFoundError):
        module.stage_safe("s3://bucket/input.SAFE", str(tmp_path), {})


def test_create_obstore_allows_http(monkeypatch, mocker, fake_obstore):
    """The http endpoints, e.g. of the local MinIO, are allowed."""
    module, _, _, _ = import_safe_to_zarr(monkeypatch, mocker)
    module.create_obstore("bucket", {"client_kwargs": {"endpoint_url": "http://minio:9000"}})
    assert fake_obstore.store_kwargs["config"] == {"endpoint": "http://minio:9000"}
    assert fake_obstore.store_kwargs["client_options"]["allow_http"] is True


@pytest.mark.parametrize(
    "error, expected",
    [
        (TimeoutError(), True),
        (ConnectionResetError(), True),
        (obstore.exceptions.GenericError("operation timed out"), True),
        (obstore.exceptions.NotFoundError("missing"), False),
        (ValueError("bad value"), False),
        (PermissionError("Access Denied"), False),
    ],
)
def test_is_transient_error(monkeypatch, mocker, error, expected):
    """Only the network errors are retried."""
    module, _, _, _ = import_safe_to_zarr(monkeypatch, mocker)
    assert module.is_transient_error(error) is expected


def test_retrying_source_staging_keeps_the_eopf_staging_of_local_products(monkeypatch, mocker):
    """The local source products, or a conversion without staging, are given to the EOPF function."""
    module, _, _, _ = import_safe_to_zarr(monkeypatch, mocker)
    eopf_convert = sys.modules["eopf.store.convert"]
    eopf_stage_source = eopf_convert._stage_source  # pylint: disable=protected-access

    with module.retrying_source_staging():
        eopf_convert._stage_source("/local/input.SAFE", {}, True, None)  # pylint: disable=protected-access
        eopf_convert._stage_source("s3://bucket/input.SAFE", {}, False, None)  # pylint: disable=protected-access

    assert eopf_stage_source.call_args_list == [
        mocker.call("/local/input.SAFE", {}, True, None),
        mocker.call("s3://bucket/input.SAFE", {}, False, None),
    ]


def test_retrying_source_staging_without_the_eopf_function(monkeypatch, mocker):
    """If the private EOPF function doesn't exist anymore, the source product is staged by EOPF."""
    module, _, _, _ = import_safe_to_zarr(monkeypatch, mocker)
    monkeypatch.delattr(sys.modules["eopf.store.convert"], "_stage_source")

    stderr = StringIO()
    with redirect_stderr(stderr), module.retrying_source_staging():
        pass
    assert "_stage_source not found" in stderr.getvalue()


def test_eopf_stage_source_signature():
    """The wrapped EOPF private function still has the arguments given by convert() (EOPF version of the cluster)."""
    eopf_convert = importlib.import_module("eopf.store.convert")
    stage_source = eopf_convert._stage_source  # pylint: disable=protected-access
    assert list(inspect.signature(stage_source).parameters) == [
        "source_path",
        "source_store_kwargs",
        "stage_source",
        "logger",
    ]


def test_stage_safe_with_a_real_obstore(monkeypatch, mocker, tmp_path):
    """Download with the real obstore functions, from a local store, and resume an interrupted download."""
    module, _, _, _ = import_safe_to_zarr(monkeypatch, mocker)
    bucket = tmp_path / "bucket"
    for path, content in SAFE_FILES.items():
        (bucket / path).parent.mkdir(parents=True, exist_ok=True)
        (bucket / path).write_bytes(content)
    monkeypatch.setattr(module, "create_obstore", lambda *_: obstore.store.LocalStore(str(bucket)))
    monkeypatch.setattr("time.sleep", lambda _: None)

    # The first download of the TIFF file is interrupted after 4 bytes
    real_get = obstore.get  # pylint: disable=no-member
    tiff = "input.SAFE/measurement/s1a-iw1-slc-vv.tiff"
    get_calls = []

    def get(store, path, options=None):
        get_calls.append((path, options))
        result = real_get(store, path, options=options)
        if path == tiff and len([call for call in get_calls if call[0] == tiff]) == 1:
            return FakeGetResult(SAFE_FILES[tiff], (4, TimeoutError()))
        return result

    monkeypatch.setattr("obstore.get", get)
    local_dir = tmp_path / "local"
    with redirect_stderr(StringIO()):
        local_safe = module.stage_safe("s3://bucket/input.SAFE", str(local_dir), {})

    assert local_safe == str(local_dir / "input.SAFE")
    for path, content in SAFE_FILES.items():
        assert (local_dir / path).read_bytes() == content
    assert [options for path, options in get_calls if path == tiff] == [None, {"range": {"offset": 4}}]
