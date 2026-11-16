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
    fake_convert = types.SimpleNamespace(convert=convert, open_datatree=mocker.Mock(name="open_datatree"))

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
        "eopf_version": "1.2.3",
        "safe_uri": cfg["safe_uri"],
        "zarr_uri": cfg["zarr_uri"],
    }


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
