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
import json
import os
import runpy
import sys
import types
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO

import pytest


def install_fake_eopf_dependencies(monkeypatch, mocker, eopf_version="3.0.0"):
    """Install mocked EOPF dependencies in sys.modules."""
    eo_config: dict[str, bool] = {}
    any_path = mocker.Mock(side_effect=lambda path, **kwargs: {"path": path, "kwargs": kwargs})
    convert = mocker.Mock()

    # safe_to_zarr imports these modules at import time, so they must exist before run_module/import_module.
    fake_eopf = types.SimpleNamespace(__version__=eopf_version)
    fake_common = types.ModuleType("eopf.common")
    # Keep the two observable EOPF calls as mocks: path construction and conversion execution.
    fake_file_utils = types.SimpleNamespace(AnyPath=any_path)
    fake_config = types.SimpleNamespace(EOConfiguration=lambda: eo_config)
    fake_store = types.ModuleType("eopf.store")
    fake_convert = types.SimpleNamespace(convert=convert)

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


def run_safe_to_zarr_as_main(monkeypatch, argv):
    """Run rs_dpr_service.safe_to_zarr as a script."""
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.delitem(sys.modules, "rs_dpr_service.safe_to_zarr", raising=False)
    return runpy.run_module("rs_dpr_service.safe_to_zarr", run_name="__main__")


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


def test_main_success(monkeypatch, mocker):
    """Convert SAFE to Zarr and print the success payload."""
    # Observe the conversion call without importing real EOPF.
    _, _, convert = install_fake_eopf_dependencies(monkeypatch, mocker)
    cfg = {"safe_uri": "s3://bucket/input.SAFE", "zarr_uri": "s3://bucket/output.zarr"}
    monkeypatch.setenv("S3_ACCESSKEY", "access")
    monkeypatch.setenv("S3_SECRETKEY", "secret")
    monkeypatch.setenv("S3_ENDPOINT", "https://example.com")
    monkeypatch.setenv("S3_REGION", "eu-west-1")

    stdout = StringIO()
    stderr = StringIO()
    # Successful execution writes only the JSON result to stdout.
    with redirect_stdout(stdout), redirect_stderr(stderr):
        run_safe_to_zarr_as_main(monkeypatch, ["safe_to_zarr.py", json.dumps(cfg)])

    assert stderr.getvalue() == ""

    expected_s3_cfg = {
        "key": "access",
        "secret": "secret",  # nosec
        "client_kwargs": {
            "endpoint_url": "https://example.com",
            "region_name": "eu-west-1",
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

    result = json.loads(stdout.getvalue())
    assert result == {
        "message": "Conversion finished",
        "eopf_version": "3.0.0",
        "safe_uri": cfg["safe_uri"],
        "zarr_uri": cfg["zarr_uri"],
    }


class FakeAnyPath:
    """Fake EOPF CPM 2.x AnyPath, with a fake S3 where only the existing paths and the touched files exist."""

    def __init__(self, path, existing, events, **kwargs):
        self.path = path
        self.kwargs = kwargs
        self.existing = existing
        self.events = events

    def __eq__(self, other):
        return isinstance(other, FakeAnyPath) and (self.path, self.kwargs) == (other.path, other.kwargs)

    def __repr__(self):
        return f"FakeAnyPath({self.path!r})"

    def __truediv__(self, other):
        return FakeAnyPath(f"{self.path}/{other}", self.existing, self.events, **self.kwargs)

    def dirname(self):
        """Parent folder"""
        return FakeAnyPath(self.path.rsplit("/", 1)[0], self.existing, self.events, **self.kwargs)

    def exists(self):
        """In S3, a folder exists if it contains an object"""
        return any(path == self.path or path.startswith(self.path + "/") for path in self.existing)

    def touch(self):
        """Create an empty object"""
        self.existing.add(self.path)
        self.events.append(("touch", self.path))

    def rm(self):
        """Remove an object"""
        self.existing.remove(self.path)
        self.events.append(("rm", self.path))


@pytest.mark.parametrize("output_dir_exists", [True, False])
def test_main_success_cpm2(monkeypatch, mocker, output_dir_exists):
    """With EOPF CPM 2.x (cpm2 Dask cluster), convert with the CPM 2.x API, in a single thread."""
    eo_config, any_path, convert = install_fake_eopf_dependencies(monkeypatch, mocker, eopf_version="2.8.1")
    existing = {"s3://bucket/out/other.zarr"} if output_dir_exists else set()
    events: list = []
    any_path.side_effect = lambda path, **kwargs: FakeAnyPath(path, existing, events, **kwargs)
    # The output folder must exist during the conversion
    convert.side_effect = lambda safe, zarr: events.append(("convert", zarr.dirname().exists()))

    cfg = {"safe_uri": "s3://bucket/input.SAFE", "zarr_uri": "s3://bucket/out/output.zarr"}
    for key, value in {
        "S3_ACCESSKEY": "access",
        "S3_SECRETKEY": "secret",
        "S3_ENDPOINT": "http://example.com",
        "S3_REGION": "eu-west-1",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("AWS_ALLOW_HTTP", raising=False)

    stdout = StringIO()
    with redirect_stdout(stdout), redirect_stderr(StringIO()):
        run_safe_to_zarr_as_main(monkeypatch, ["safe_to_zarr.py", json.dumps(cfg)])

    expected_s3_cfg = {
        "key": "access",
        "secret": "secret",  # nosec
        "client_kwargs": {
            "endpoint_url": "http://example.com",
            "region_name": "eu-west-1",
        },
    }
    # The SAFE and Zarr paths are given as AnyPath with the S3 credentials, without CPM 3.x kwargs
    convert.assert_called_once_with(
        FakeAnyPath(cfg["safe_uri"], existing, events, **expected_s3_cfg),
        FakeAnyPath(cfg["zarr_uri"], existing, events, **expected_s3_cfg),
    )
    # A marker object is created in the missing output folder, and removed after the conversion
    if output_dir_exists:
        assert events == [("convert", True)]
    else:
        assert events == [("touch", "s3://bucket/out/.keep"), ("convert", True), ("rm", "s3://bucket/out/.keep")]
    assert eo_config == {"store__convert__use_multithreading": False}
    # The obstore setting is only for CPM 3.x
    assert "AWS_ALLOW_HTTP" not in os.environ
    assert json.loads(stdout.getvalue())["eopf_version"] == "2.8.1"


def test_main_writes_the_result_pipe(monkeypatch, mocker):
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


def test_main_conversion_failure_exits(monkeypatch, mocker):
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
