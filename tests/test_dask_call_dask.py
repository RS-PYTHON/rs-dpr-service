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

"""Tests for rs_dpr_service.dask.call_dask helpers."""

import json
import os
import sys
import types
import zipfile
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from rs_dpr_service.dask import call_dask


def _make_processor_caller(mocker, use_mockup=False):
    """Create ProcessorCaller with the minimal worker-only imports stubbed."""
    fake_s3fs = types.ModuleType("s3fs")
    setattr(fake_s3fs, "S3FileSystem", type("S3FileSystem", (), {}))
    mocker.patch.dict(sys.modules, {"s3fs": fake_s3fs})

    return call_dask.ProcessorCaller(
        caller_env={
            "S3_ACCESSKEY": "access-key",
            "S3_SECRETKEY": "secret-key",
            "S3_ENDPOINT": "https://s3.test",
            "S3_REGION": "eu-west-1",
            "OTEL_RESOURCE_ATTRIBUTES": "service.name=test",
        },
        span_context=mocker.Mock(),
        cluster_address="http://dask-gateway.test",
        cluster_info=call_dask.ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
        processor_name="mockup" if use_mockup else "s1_l0",
        job_id="job-1",
        data={"payload": "value"},
    )


def test_get_ip_address_resolves_hostname(mocker):
    """Test get_ip_address() resolves the current hostname."""
    mocker.patch("rs_dpr_service.dask.call_dask.socket.gethostname", return_value="worker-host")
    gethostbyname = mocker.patch(
        "rs_dpr_service.dask.call_dask.socket.gethostbyname",
        return_value="10.0.0.5",
    )

    assert call_dask.get_ip_address() == "10.0.0.5"
    gethostbyname.assert_called_once_with("worker-host")


def test_processor_caller_hide_secrets_masks_sensitive_log_values(mocker):
    """Test ProcessorCaller.hide_secrets() masks sensitive values in log payloads."""
    caller = _make_processor_caller(mocker)
    # The log mimics payload/S3 credentials printed by EOPF before being forwarded to our logs.
    log = (
        "{"
        "'key': 'access-key', "
        "'secret': 'secret-key', "
        "'endpoint_url': 'https://s3.test', "
        "'region_name': 'eu-west-1', "
        "'api_token': 'jupyter-token', "
        "'password': 'local-password', "
        "'safe_value': 'visible'"
        "}"
    )

    masked = caller.hide_secrets(log)

    # Sensitive fields should be redacted, while unrelated values stay visible.
    assert "'key': ***" in masked
    assert "'secret': ***" in masked
    assert "'endpoint_url': ***" in masked
    assert "'region_name': ***" in masked
    assert "'api_token': ***" in masked
    assert "'password': ***" in masked
    assert "'safe_value': 'visible'" in masked


def test_upload_this_module_uploads_zip_and_reload_task_to_dask_workers(mocker, tmp_path):
    """Test upload_this_module() creates the worker zip and submits cleanup/reload tasks."""
    worker_dir = tmp_path / "worker"
    worker_dir.mkdir()
    previous_zip = worker_dir / "rs_dpr_service.zip"
    previous_zip.write_text("old zip", encoding="utf-8")
    worker = SimpleNamespace(local_directory=str(worker_dir))

    # Mock client.run(): Dask would execute these closures on workers, so run them locally here.
    def run_side_effect(worker_function):
        """Run the submitted Dask worker function locally."""
        if worker_function.__name__ == "remove_previous_zip":
            return {"worker-1": worker_function(worker)}
        return {"worker-1": worker_function()}

    dask_client = mocker.Mock()
    dask_client.run.side_effect = run_side_effect

    def upload_file_side_effect(zip_path):
        """Verify the generated zip contains the modules expected by Dask workers."""
        assert os.path.isfile(zip_path)
        # The uploaded archive must contain the worker-side code used later by Dask tasks.
        with zipfile.ZipFile(zip_path) as zipped:
            assert "rs_dpr_service/dask/call_dask.py" in zipped.namelist()
            assert "rs_dpr_service/utils/settings.py" in zipped.namelist()

    dask_client.upload_file.side_effect = upload_file_side_effect
    reload_mock = mocker.patch("rs_dpr_service.dask.call_dask.reload", side_effect=lambda module: module)

    call_dask.upload_this_module(dask_client)

    assert not previous_zip.exists()
    dask_client.upload_file.assert_called_once()
    assert dask_client.run.call_count == 2
    assert reload_mock.called


def test_upload_this_module_ignores_upload_key_error(mocker):
    """Test upload_this_module() ignores the Dask upload KeyError seen during worker scale-up."""
    dask_client = mocker.Mock()
    dask_client.run.return_value = {}
    # Dask can raise this while new workers are added; production code intentionally ignores it.
    dask_client.upload_file.side_effect = KeyError("worker already has the zip")

    call_dask.upload_this_module(dask_client)

    dask_client.upload_file.assert_called_once()
    assert dask_client.run.call_count == 2


def test_convert_safe_to_zarr_runs_safe_to_zarr_module(mocker, monkeypatch, tmp_path):
    """Test convert_safe_to_zarr() calls the safe_to_zarr module from the uploaded worker zip."""
    zip_path = tmp_path / "rs_dpr_service.zip"
    zip_path.write_text("fake zip content", encoding="utf-8")
    # convert_safe_to_zarr() derives the uploaded zip path from call_dask.__file__ inside the worker.
    monkeypatch.setattr(
        call_dask,
        "__file__",
        str(zip_path / "rs_dpr_service" / "dask" / "call_dask.py"),
    )

    completed = SimpleNamespace(returncode=0, stdout="converted\n", stderr="")
    subprocess_run = mocker.patch("rs_dpr_service.dask.call_dask.subprocess.run", return_value=completed)
    cfg = {"safe_uri": "s3://bucket/input.SAFE", "zarr_uri": "s3://bucket/output.zarr"}

    result = call_dask.convert_safe_to_zarr(cfg)

    assert result == "converted"
    subprocess_run.assert_called_once()
    command = subprocess_run.call_args.args[0]
    env = subprocess_run.call_args.kwargs["env"]
    assert command[:3] == [call_dask.sys.executable, "-m", "rs_dpr_service.safe_to_zarr"]
    assert json.loads(command[3]) == cfg
    assert env["PYTHONPATH"].split(os.pathsep)[0] == str(zip_path)
    assert subprocess_run.call_args.kwargs["check"] is False


def test_convert_safe_to_zarr_raises_when_uploaded_zip_is_missing(monkeypatch, tmp_path):
    """Test convert_safe_to_zarr() fails clearly when the worker zip cannot be found."""
    zip_path = tmp_path / "missing_rs_dpr_service.zip"
    # Point __file__ inside a non-existing uploaded zip to cover the early validation error.
    monkeypatch.setattr(
        call_dask,
        "__file__",
        str(zip_path / "rs_dpr_service" / "dask" / "call_dask.py"),
    )

    with pytest.raises(RuntimeError, match="Cannot locate rs_dpr_service.zip"):
        call_dask.convert_safe_to_zarr({"safe_uri": "input.SAFE"})


def test_convert_safe_to_zarr_raises_when_subprocess_fails(mocker, monkeypatch, tmp_path):
    """Test convert_safe_to_zarr() raises the subprocess stderr on conversion failure."""
    zip_path = tmp_path / "rs_dpr_service.zip"
    zip_path.write_text("fake zip content", encoding="utf-8")
    # Keep the zip discovery branch successful, then force the conversion subprocess to fail.
    monkeypatch.setattr(
        call_dask,
        "__file__",
        str(zip_path / "rs_dpr_service" / "dask" / "call_dask.py"),
    )
    mocker.patch(
        "rs_dpr_service.dask.call_dask.subprocess.run",
        return_value=SimpleNamespace(returncode=1, stdout="", stderr="conversion failed"),
    )

    with pytest.raises(RuntimeError, match="Conversion failed: conversion failed"):
        call_dask.convert_safe_to_zarr({"safe_uri": "input.SAFE"})


def test_processor_caller_get_tasktable_returns_imported_processor_tasktable(mocker):
    """Test ProcessorCaller.get_tasktable() nominally loads a processor class and returns its tasktable."""
    caller = _make_processor_caller(mocker)
    # Env propagation is covered by run_processor(); keep this test focused on tasktable loading.
    caller.copy_caller_env = mocker.Mock()

    @contextmanager
    def fake_start_span(*args, **kwargs):
        """Yield a fake span around the tasktable call."""
        assert args == ("rs_dpr_service.dask.call_dask", "dpr_dask_tasktable_FakeProcessor", caller.span_context)
        assert not kwargs
        yield mocker.Mock()

    class FakeProcessor:
        """Fake EOPF processor exposing the tasktable API used by ProcessorCaller."""

        @staticmethod
        def get_available_modes():
            """Return available modes for debug logging."""
            return ["default"]

        @staticmethod
        def get_default_mode():
            """Return the mode used to fetch the tasktable."""
            return "default"

        @staticmethod
        def get_tasktable_description(mode):
            """Return the tasktable for the selected mode."""
            assert mode == "default"
            return {"tasks": [{"name": "task-1"}]}

    # Tracing is external to the tasktable behavior, but the span name/context are still asserted above.
    init_traces = mocker.patch("rs_dpr_service.dask.call_dask.init_traces")
    mocker.patch("rs_dpr_service.dask.call_dask.start_span", side_effect=fake_start_span)
    # Patch import_module last because mocker.patch itself uses importlib internally.
    # The fake module mimics the EOPF processor module loaded dynamically on Dask workers.
    module = SimpleNamespace(FakeProcessor=FakeProcessor)
    import_module = mocker.patch("rs_dpr_service.dask.call_dask.importlib.import_module", return_value=module)

    result = caller.get_tasktable("fake.module", "FakeProcessor")

    caller.copy_caller_env.assert_called_once_with()
    init_traces.assert_called_once_with(None, call_dask.SERVICE_NAME)
    import_module.assert_called_once_with("fake.module")
    assert result == {"tasks": [{"name": "task-1"}]}


def test_processor_caller_get_tasktable_uses_normal_import_path_for_mockup(mocker):
    """Test ProcessorCaller.get_tasktable() uses the normal dynamic import path for mockup."""
    caller = _make_processor_caller(mocker, use_mockup=True)
    caller.copy_caller_env = mocker.Mock()

    class MockupProcessor:
        """Fake EOPF processor class used to return a mockup tasktable."""

        @staticmethod
        def get_available_modes():
            """Return fake available modes."""
            return ["default"]

        @staticmethod
        def get_default_mode():
            """Return fake default mode."""
            return "default"

        @staticmethod
        def get_tasktable_description(_mode):
            """Return a fake tasktable."""
            return {"mockup": True}

    @contextmanager
    def fake_start_span(*args, **kwargs):
        """Yield a fake span around the mockup tasktable call."""
        assert args == ("rs_dpr_service.dask.call_dask", "dpr_dask_tasktable_MockupProcessor", caller.span_context)
        assert not kwargs
        yield mocker.Mock()

    init_traces = mocker.patch("rs_dpr_service.dask.call_dask.init_traces")
    mocker.patch("rs_dpr_service.dask.call_dask.start_span", side_effect=fake_start_span)
    # Patch import_module last because mocker.patch itself uses importlib internally.
    module = SimpleNamespace(MockupProcessor=MockupProcessor)
    import_module = mocker.patch("rs_dpr_service.dask.call_dask.importlib.import_module", return_value=module)

    result = caller.get_tasktable("fake.module", "MockupProcessor")

    caller.copy_caller_env.assert_called_once_with()
    init_traces.assert_called_once_with(None, call_dask.SERVICE_NAME)
    import_module.assert_called_once_with("fake.module")
    assert result == {"mockup": True}


def test_processor_caller_run_processor_runs_nominal_orchestration(mocker, monkeypatch):
    """Test ProcessorCaller.run_processor() copies env, runs init/trigger/finalize, and returns the result."""
    # Track env restoration even though copy_caller_env() writes directly into os.environ.
    for key in (
        "S3_ACCESSKEY",
        "S3_SECRETKEY",
        "S3_ENDPOINT",
        "S3_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_ENDPOINT_URL_S3",
        "AWS_DEFAULT_REGION",
        "AWS_DEFAULT_OUTPUT",
        "OTEL_RESOURCE_ATTRIBUTES",
    ):
        monkeypatch.setenv(key, "")

    caller = _make_processor_caller(mocker)
    expected_result = {"result": "ok"}
    # Keep run_processor() real, but stop before real EOPF init/subprocess/final upload.
    caller.init = mocker.Mock()
    caller.trigger = mocker.Mock()
    caller.finalize = mocker.Mock(return_value=expected_result)

    @contextmanager
    def fake_start_span(*args, **kwargs):
        """Yield a fake span around the processor run."""
        assert args == ("rs_dpr_service.dask.call_dask", "dpr_dask_processor", caller.span_context)
        assert not kwargs
        yield mocker.Mock()

    # copy_caller_env() remains real, so these mocks only remove external tracing/network noise.
    init_traces = mocker.patch("rs_dpr_service.dask.call_dask.init_traces")
    mocker.patch("rs_dpr_service.dask.call_dask.start_span", side_effect=fake_start_span)
    mocker.patch("rs_dpr_service.dask.call_dask.reload", side_effect=lambda module: module)
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")

    result = caller.run_processor()

    init_traces.assert_called_once_with(None, call_dask.SERVICE_NAME)
    caller.init.assert_called_once_with()
    caller.trigger.assert_called_once_with()
    caller.finalize.assert_called_once_with()
    assert result == expected_result
    assert os.environ["AWS_ACCESS_KEY_ID"] == "access-key"
    assert os.environ["AWS_SECRET_ACCESS_KEY"] == "secret-key"
    assert os.environ["AWS_ENDPOINT_URL_S3"] == "https://s3.test"
    assert os.environ["AWS_DEFAULT_REGION"] == "eu-west-1"
    assert os.environ["AWS_DEFAULT_OUTPUT"] == "json"
    assert os.environ["OTEL_RESOURCE_ATTRIBUTES"] == "service.name=test"
    assert caller.exec_times[0][0] == "Run processor"


def test_processor_caller_run_processor_finalizes_and_records_error_when_trigger_fails(mocker):
    """Test ProcessorCaller.run_processor() finalizes and records errors when trigger() fails."""
    caller = _make_processor_caller(mocker)
    # Isolate the error branch: only trigger() fails, cleanup/finalize stays observable.
    caller.copy_caller_env = mocker.Mock()
    caller.init = mocker.Mock()
    caller.trigger = mocker.Mock(side_effect=RuntimeError("trigger failed"))
    caller.finalize = mocker.Mock(return_value={})
    span = mocker.Mock()

    @contextmanager
    def fake_start_span(*args, **kwargs):
        """Yield a fake span around the failing processor run."""
        assert args == ("rs_dpr_service.dask.call_dask", "dpr_dask_processor", caller.span_context)
        assert not kwargs
        yield span

    init_traces = mocker.patch("rs_dpr_service.dask.call_dask.init_traces")
    record_error = mocker.patch("rs_dpr_service.dask.call_dask.record_error")
    mocker.patch("rs_dpr_service.dask.call_dask.start_span", side_effect=fake_start_span)
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")

    with pytest.raises(RuntimeError, match="trigger failed"):
        caller.run_processor()

    caller.copy_caller_env.assert_called_once_with()
    init_traces.assert_called_once_with(None, call_dask.SERVICE_NAME)
    caller.init.assert_called_once_with()
    caller.trigger.assert_called_once_with()
    # finalize() must still run in the exception branch to preserve cleanup behavior.
    caller.finalize.assert_called_once_with()
    record_error.assert_called_once_with(span, caller.trigger.side_effect)


def test_processor_caller_run_processor_uses_normal_orchestration_for_mockup(mocker):
    """Test ProcessorCaller.run_processor() uses normal init/trigger/finalize orchestration for mockup."""
    caller = _make_processor_caller(mocker, use_mockup=True)
    caller.copy_caller_env = mocker.Mock()
    caller.init = mocker.Mock()
    caller.trigger = mocker.Mock()
    caller.finalize = mocker.Mock(return_value={"mockup": "ok"})

    @contextmanager
    def fake_start_span(*args, **kwargs):
        """Yield a fake span around the mockup processor run."""
        assert args == ("rs_dpr_service.dask.call_dask", "dpr_dask_processor", caller.span_context)
        assert not kwargs
        yield mocker.Mock()

    init_traces = mocker.patch("rs_dpr_service.dask.call_dask.init_traces")
    mocker.patch("rs_dpr_service.dask.call_dask.start_span", side_effect=fake_start_span)
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")

    result = caller.run_processor()

    init_traces.assert_called_once_with(None, call_dask.SERVICE_NAME)
    caller.init.assert_called_once_with()
    caller.trigger.assert_called_once_with()
    caller.finalize.assert_called_once_with()
    assert result == {"mockup": "ok"}


def test_processor_caller_run_processor_returns_normal_finalize_value_for_mockup(mocker):
    """Test ProcessorCaller.run_processor() returns the normal finalize() value for mockup."""
    caller = _make_processor_caller(mocker, use_mockup=True)
    caller.copy_caller_env = mocker.Mock()
    caller.init = mocker.Mock()
    caller.trigger = mocker.Mock()

    @contextmanager
    def fake_start_span(*args, **kwargs):
        """Yield a fake span around the mockup finalize run."""
        assert args == ("rs_dpr_service.dask.call_dask", "dpr_dask_processor", caller.span_context)
        assert not kwargs
        yield mocker.Mock()

    mocker.patch("rs_dpr_service.dask.call_dask.init_traces")
    mocker.patch("rs_dpr_service.dask.call_dask.start_span", side_effect=fake_start_span)
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")

    result = caller.run_processor()

    caller.init.assert_called_once_with()
    caller.trigger.assert_called_once_with()
    assert result == {}


def test_processor_caller_trigger_uses_eorunner_when_local_cluster_is_enabled(mocker, monkeypatch):
    """Test ProcessorCaller.trigger() calls EORunner directly in local mode with local cluster enabled."""
    caller = _make_processor_caller(mocker)
    caller.experimental_config = call_dask.ExperimentalConfig(local_cluster={"enabled": True})
    caller.payload_contents = {"workflow": [{"name": "unit"}]}

    eopf_module = types.ModuleType("eopf")
    triggering_module = types.ModuleType("eopf.triggering")
    runner_module = types.ModuleType("eopf.triggering.runner")
    eorunner = mocker.Mock()
    eorunner_class = mocker.Mock(return_value=eorunner)
    runner_module.EORunner = eorunner_class
    monkeypatch.setitem(sys.modules, "eopf", eopf_module)
    monkeypatch.setitem(sys.modules, "eopf.triggering", triggering_module)
    monkeypatch.setitem(sys.modules, "eopf.triggering.runner", runner_module)
    monkeypatch.setattr(call_dask.settings, "LOCAL_MODE", True)
    launch_subprocess = mocker.patch("rs_dpr_service.dask.call_dask.ProcessorCaller._launch_eopf_subprocess")

    caller.trigger()

    eorunner_class.assert_called_once_with()
    eorunner.run.assert_called_once_with(caller.payload_contents)
    launch_subprocess.assert_not_called()


# ---- _collect_storage_options ----


def test_collect_storage_options_returns_empty_when_no_io_section():
    """Returns an empty list when the payload has no I/O section."""
    assert not call_dask.ProcessorCaller._collect_storage_options({})  # pylint: disable=protected-access


def test_collect_storage_options_gathers_all_sections():
    """Collects storage_options from input_products, output_products, and adfs."""
    so_in = {"key": "k1", "secret": "s1"}  # nosec B105
    so_out = {"key": "k2", "secret": "s2"}  # nosec B105
    so_adf = {"key": "k3", "secret": "s3"}  # nosec B105
    payload = {
        "I/O": {
            "input_products": [{"reader_params": {"storage_options": so_in}}],
            "output_products": [{"writer_params": {"storage_options": so_out}}],
            "adfs": [{"adf_params": {"storage_options": so_adf}}],
        },
    }
    result = call_dask.ProcessorCaller._collect_storage_options(payload)  # pylint: disable=protected-access
    assert result == [so_in, so_out, so_adf]


def test_collect_storage_options_skips_items_without_storage_options():
    """Items whose params dict has no storage_options key are excluded."""
    payload: dict[str, object] = {
        "I/O": {
            "input_products": [{"reader_params": {}}],
            "output_products": [{"writer_params": {}}],
            "adfs": [{"adf_params": {}}],
        },
    }
    assert not call_dask.ProcessorCaller._collect_storage_options(payload)  # pylint: disable=protected-access


def test_collect_storage_options_falls_back_to_store_params():
    """Uses store_params when reader_params / writer_params / adf_params are absent."""
    so = {"key": "k"}
    payload = {
        "I/O": {
            "input_products": [{"store_params": {"storage_options": so}}],
            "output_products": [{"store_params": {"storage_options": so}}],
            "adfs": [{"store_params": {"storage_options": so}}],
        },
    }
    result = call_dask.ProcessorCaller._collect_storage_options(payload)  # pylint: disable=protected-access
    assert result == [so, so, so]


def test_collect_storage_options_uses_lowercase_io_key_fallback():
    """Falls back to the 'io' key when 'I/O' is absent."""
    so = {"key": "k"}
    payload = {"io": {"input_products": [{"reader_params": {"storage_options": so}}]}}
    result = call_dask.ProcessorCaller._collect_storage_options(payload)  # pylint: disable=protected-access
    assert result == [so]


# ---- write_secret_conf_files ----


def test_write_secret_conf_files_errors_on_multiple_files(mocker, tmp_path):
    """Logs an error and writes nothing when more than one secret file is requested."""
    caller = _make_processor_caller(mocker)
    error_log = mocker.patch("rs_dpr_service.dask.call_dask.logger.error")

    caller.write_secret_conf_files(["a.json", "b.json"], {}, str(tmp_path / "payload.yaml"))

    error_log.assert_called_once()
    assert not list(tmp_path.iterdir())


def test_write_secret_conf_files_warns_when_no_storage_options(mocker, tmp_path):
    """Logs a warning and writes nothing when the payload contains no storage_options."""
    caller = _make_processor_caller(mocker)
    warn_log = mocker.patch("rs_dpr_service.dask.call_dask.logger.warning")

    caller.write_secret_conf_files(["secrets.json"], {}, str(tmp_path / "payload.yaml"))

    warn_log.assert_called_once()
    assert not (tmp_path / "secrets.json").exists()


def test_write_secret_conf_files_errors_on_multiple_credential_sets(mocker, tmp_path):
    """Logs an error and writes nothing when differing credentials are found across products."""
    caller = _make_processor_caller(mocker)
    payload = {
        "I/O": {
            "input_products": [
                {
                    "reader_params": {
                        "storage_options": {
                            "key": "k1",
                            "secret": "s1",  # nosec B105
                            "client_kwargs": {"endpoint_url": "u1", "region_name": "r1"},
                        },
                    },
                },
                {
                    "reader_params": {
                        "storage_options": {
                            "key": "k2",
                            "secret": "s2",  # nosec B105
                            "client_kwargs": {"endpoint_url": "u2", "region_name": "r2"},
                        },
                    },
                },
            ],
        },
    }
    error_log = mocker.patch("rs_dpr_service.dask.call_dask.logger.error")

    caller.write_secret_conf_files(["secrets.json"], payload, str(tmp_path / "payload.yaml"))

    error_log.assert_called_once()
    assert not (tmp_path / "secrets.json").exists()


def test_write_secret_conf_files_nominal(mocker, tmp_path):
    """Writes secrets.json with the correct structure when all credentials are identical."""
    caller = _make_processor_caller(mocker)
    creds = {
        "key": "mykey",
        "secret": "mysecret",  # nosec B105
        "client_kwargs": {"endpoint_url": "https://s3.test", "region_name": "eu-west"},
    }
    payload = {
        "I/O": {
            "input_products": [{"reader_params": {"storage_options": creds}}],
            "output_products": [{"writer_params": {"storage_options": creds}}],
            "adfs": [{"adf_params": {"storage_options": creds}}],
        },
    }

    caller.write_secret_conf_files(["secrets.json"], payload, str(tmp_path / "payload.yaml"))

    secrets_path = tmp_path / "secrets.json"
    assert secrets_path.exists()
    assert json.loads(secrets_path.read_text(encoding="utf-8")) == {
        "s3": {
            "key": "mykey",
            "secret": "mysecret",  # nosec B105
            "client_kwargs": {"endpoint_url": "https://s3.test", "region_name": "eu-west"},
        },
    }


def test_write_secret_conf_files_written_next_to_payload(mocker, tmp_path):
    """Secrets file is created in the payload directory, not the process cwd."""
    caller = _make_processor_caller(mocker)
    payload_dir = tmp_path / "subdir"
    payload_dir.mkdir()
    creds = {
        "key": "k",
        "secret": "s",  # nosec B105
        "client_kwargs": {"endpoint_url": "u", "region_name": "r"},
    }
    payload = {"I/O": {"input_products": [{"reader_params": {"storage_options": creds}}]}}

    caller.write_secret_conf_files(["secrets.json"], payload, str(payload_dir / "payload.yaml"))

    assert (payload_dir / "secrets.json").exists()
    assert not (tmp_path / "secrets.json").exists()
