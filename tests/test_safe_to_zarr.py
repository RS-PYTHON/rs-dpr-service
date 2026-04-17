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

"""Unit tests for rs_dpr_service.safe_to_zarr."""

import importlib
import json
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

    fake_eopf = types.SimpleNamespace(__version__="1.2.3")
    fake_common = types.ModuleType("eopf.common")
    fake_file_utils = types.SimpleNamespace(AnyPath=any_path)
    fake_config = types.SimpleNamespace(EOConfiguration=lambda: eo_config)
    fake_store = types.ModuleType("eopf.store")
    fake_convert = types.SimpleNamespace(convert=convert)

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
    install_fake_eopf_dependencies(monkeypatch, mocker)

    stdout = StringIO()
    stderr = StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr), pytest.raises(SystemExit) as exc:
        run_safe_to_zarr_as_main(monkeypatch, ["safe_to_zarr.py"])

    assert exc.value.code == 1
    assert stdout.getvalue() == ""
    assert "Usage: python safe_to_zarr.py" in stderr.getvalue()


def test_main_with_invalid_json_exits(monkeypatch, mocker):
    """Exit with an error when the JSON payload cannot be decoded."""
    install_fake_eopf_dependencies(monkeypatch, mocker)

    stdout = StringIO()
    stderr = StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr), pytest.raises(SystemExit) as exc:
        run_safe_to_zarr_as_main(monkeypatch, ["safe_to_zarr.py", "{not-json}"])

    assert exc.value.code == 1
    assert stdout.getvalue() == ""
    assert "Failed to decode config JSON" in stderr.getvalue()


def test_main_success(monkeypatch, mocker):
    """Convert SAFE to Zarr and print the success payload."""
    eo_config, any_path, convert = install_fake_eopf_dependencies(monkeypatch, mocker)
    cfg = {"safe_uri": "s3://bucket/input.SAFE", "zarr_uri": "s3://bucket/output.zarr"}
    monkeypatch.setenv("S3_ACCESSKEY", "access")
    monkeypatch.setenv("S3_SECRETKEY", "secret")
    monkeypatch.setenv("S3_ENDPOINT", "https://example.com")
    monkeypatch.setenv("S3_REGION", "eu-west-1")

    stdout = StringIO()
    stderr = StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        run_safe_to_zarr_as_main(monkeypatch, ["safe_to_zarr.py", json.dumps(cfg)])

    assert stderr.getvalue() == ""
    assert eo_config["store__convert__use_multithreading"] is False

    expected_s3_cfg = {
        "key": "access",
        "secret": "secret",  # nosec B105
        "client_kwargs": {
            "endpoint_url": "https://example.com",
            "region_name": "eu-west-1",
        },
    }
    safe = {"path": cfg["safe_uri"], "kwargs": expected_s3_cfg}
    zarr = {"path": cfg["zarr_uri"], "kwargs": expected_s3_cfg}
    assert any_path.call_args_list == [
        mocker.call(cfg["safe_uri"], **expected_s3_cfg),
        mocker.call(cfg["zarr_uri"], **expected_s3_cfg),
    ]
    convert.assert_called_once_with(safe, zarr)

    result = json.loads(stdout.getvalue())
    assert result == {
        "message": "Conversion finished",
        "eopf_version": "1.2.3",
        "safe_uri": cfg["safe_uri"],
        "zarr_uri": cfg["zarr_uri"],
    }


def test_main_conversion_failure_exits(monkeypatch, mocker):
    """Exit with an error when the conversion raises an exception."""
    _, _, convert = install_fake_eopf_dependencies(monkeypatch, mocker)
    cfg = {"safe_uri": "s3://bucket/input.SAFE", "zarr_uri": "s3://bucket/output.zarr"}
    monkeypatch.setenv("S3_ACCESSKEY", "access")
    monkeypatch.setenv("S3_SECRETKEY", "secret")
    monkeypatch.setenv("S3_ENDPOINT", "https://example.com")
    monkeypatch.setenv("S3_REGION", "eu-west-1")
    convert.side_effect = RuntimeError("boom")

    stdout = StringIO()
    stderr = StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr), pytest.raises(SystemExit) as exc:
        run_safe_to_zarr_as_main(monkeypatch, ["safe_to_zarr.py", json.dumps(cfg)])

    assert exc.value.code == 1
    assert stdout.getvalue() == ""
    assert "Conversion failed safe_to_zarr: boom" in stderr.getvalue()
