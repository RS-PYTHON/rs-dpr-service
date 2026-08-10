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
from pathlib import Path
from types import SimpleNamespace

import pytest

from rs_dpr_service.dask import call_dask

from .conftest import get_cluster_info


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
        dask_gateway_address="http://dask-gateway.test",
        cluster_info=get_cluster_info(),
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
    init_traces.assert_called_once_with()
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
    init_traces.assert_called_once_with()
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
        assert args == ("rs_dpr_service.dask.call_dask", "[dask-l0] dpr_dask_processor", caller.span_context)
        assert not kwargs
        yield mocker.Mock()

    # copy_caller_env() remains real, so these mocks only remove external tracing/network noise.
    init_traces = mocker.patch("rs_dpr_service.dask.call_dask.init_traces")
    mocker.patch("rs_dpr_service.dask.call_dask.start_span", side_effect=fake_start_span)
    mocker.patch("rs_dpr_service.dask.call_dask.reload", side_effect=lambda module: module)
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")

    result = caller.run_processor()

    init_traces.assert_called_once_with()
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
        assert args == ("rs_dpr_service.dask.call_dask", "[dask-l0] dpr_dask_processor", caller.span_context)
        assert not kwargs
        yield span

    init_traces = mocker.patch("rs_dpr_service.dask.call_dask.init_traces")
    record_error = mocker.patch("rs_dpr_service.dask.call_dask.record_error")
    mocker.patch("rs_dpr_service.dask.call_dask.start_span", side_effect=fake_start_span)
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")

    with pytest.raises(RuntimeError, match="trigger failed"):
        caller.run_processor()

    caller.copy_caller_env.assert_called_once_with()
    init_traces.assert_called_once_with()
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
        assert args == ("rs_dpr_service.dask.call_dask", "[dask-l0] dpr_dask_processor", caller.span_context)
        assert not kwargs
        yield mocker.Mock()

    init_traces = mocker.patch("rs_dpr_service.dask.call_dask.init_traces")
    mocker.patch("rs_dpr_service.dask.call_dask.start_span", side_effect=fake_start_span)
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")

    result = caller.run_processor()

    init_traces.assert_called_once_with()
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
        assert args == ("rs_dpr_service.dask.call_dask", "[dask-l0] dpr_dask_processor", caller.span_context)
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
    caller.experimental_config = call_dask.ExperimentalConfig(
        local_cluster=call_dask.ExperimentalConfig.LocalCluster(enabled=True),
    )
    caller.payload_contents = {"workflow": [{"name": "unit"}]}

    eopf_module = types.ModuleType("eopf")
    triggering_module = types.ModuleType("eopf.triggering")
    runner_module = types.ModuleType("eopf.triggering.runner")
    eorunner = mocker.Mock()
    eorunner_class = mocker.Mock(return_value=eorunner)
    setattr(runner_module, "EORunner", eorunner_class)
    monkeypatch.setitem(sys.modules, "eopf", eopf_module)
    monkeypatch.setitem(sys.modules, "eopf.triggering", triggering_module)
    monkeypatch.setitem(sys.modules, "eopf.triggering.runner", runner_module)
    monkeypatch.setattr(call_dask.settings, "LOCAL_MODE", True)
    launch_subprocess = mocker.patch("rs_dpr_service.dask.call_dask.ProcessorCaller._launch_eopf_subprocess")

    caller.trigger()

    eorunner_class.assert_called_once_with()
    eorunner.run.assert_called_once_with(caller.payload_contents)
    launch_subprocess.assert_not_called()


# ---- _launch_eopf_subprocess / _flush_log_batch ----


def _fake_subprocess(mocker, lines: list[str], returncode: int = 0):
    """Build a fake Popen context manager yielding the given stdout lines then EOF.

    select.select() is patched to always report data ready immediately, so these lines land in
    the batch back-to-back without any real wait: only the _LOG_BATCH_SIZE threshold, not the
    time-based flush, is under test here.
    """
    fake_stdout = mocker.Mock()
    fake_stdout.readline.side_effect = [*lines, ""]
    always_ready: tuple[list, list, list] = ([fake_stdout], [], [])
    mocker.patch("rs_dpr_service.dask.call_dask.select.select", return_value=always_ready)

    fake_proc = mocker.MagicMock()
    fake_proc.__enter__.return_value = fake_proc
    fake_proc.__exit__.return_value = False
    fake_proc.stdout = fake_stdout
    fake_proc.pid = 1234
    fake_proc.returncode = returncode
    fake_proc.wait.return_value = returncode
    return fake_proc


def test_launch_eopf_subprocess_batches_forwarded_log_lines_and_flushes_remainder(mocker, monkeypatch, tmp_path):
    """Test _launch_eopf_subprocess groups subprocess output lines into batched logger calls.

    Forwarding one dask log record per subprocess output line floods the rs-dpr-service caller
    (Client.forward_logging() re-processes every record live) and can starve it under its
    Kubernetes CPU limit during a verbose processor run. Lines must be coalesced into batches.
    """
    caller = _make_processor_caller(mocker)
    caller.log_path = str(tmp_path / "eopf.log")
    caller.working_dir = str(tmp_path)
    batch_size = call_dask.ProcessorCaller._LOG_BATCH_SIZE  # pylint: disable=protected-access

    # One full batch, plus a partial remainder, plus a blank line that must be skipped entirely.
    body_lines = [f"line {i}\n" for i in range(batch_size)] + ["last line\n", "\n"]
    fake_proc = _fake_subprocess(mocker, body_lines)

    mocker.patch("rs_dpr_service.dask.call_dask.subprocess.Popen", return_value=fake_proc)
    mocker.patch("rs_dpr_service.dask.call_dask.get_client", side_effect=ValueError)
    monkeypatch.setattr(call_dask.settings, "CLUSTER_MODE", False)
    logger_info = mocker.patch("rs_dpr_service.dask.call_dask.logger.info")
    span = mocker.Mock()

    caller._launch_eopf_subprocess(span, {})  # pylint: disable=protected-access

    job_prefix = f"[JOB:{caller.job_id}]"
    all_calls = [call.args[0] for call in logger_info.call_args_list]
    batch_calls = [msg for msg in all_calls if msg.startswith(job_prefix)]
    # One flush once the batch fills up, one final flush for the single remaining line.
    assert batch_calls == [
        "[JOB:job-1] " + "\n".join(f"line {i}" for i in range(batch_size)),
        "[JOB:job-1] last line",
    ]

    # Every raw line (including the blank one) is still written to the local report file.
    assert Path(caller.log_path).read_text(encoding="utf-8") == "".join(body_lines)

    fake_proc.wait.assert_called_once_with()
    span.set_status.assert_called_once()
    assert span.set_status.call_args.args[0].status_code == call_dask.StatusCode.OK


def test_launch_eopf_subprocess_flushes_a_partial_batch_after_an_idle_period(mocker, monkeypatch, tmp_path):
    """Test _launch_eopf_subprocess flushes a buffered line once it has been idle for too long.

    Without a time bound, a single buffered line would only be flushed once _LOG_BATCH_SIZE
    lines pile up, or the processor exits: for a quiet processor (e.g. one output line every 30
    minutes), that would leave the line invisible on the SSE log stream for a very long time.
    """
    caller = _make_processor_caller(mocker)
    caller.log_path = str(tmp_path / "eopf.log")
    caller.working_dir = str(tmp_path)

    fake_stdout = mocker.Mock()
    fake_stdout.readline.side_effect = ["first line\n", ""]
    fake_proc = mocker.MagicMock()
    fake_proc.__enter__.return_value = fake_proc
    fake_proc.__exit__.return_value = False
    fake_proc.stdout = fake_stdout
    fake_proc.pid = 1234
    fake_proc.returncode = 0
    fake_proc.wait.return_value = 0

    # select() reports: data ready (the one line), then two idle timeouts, then EOF ready.
    # A real idle select() only returns once `timeout` seconds have elapsed, so the fake clock
    # is advanced by `timeout` on every simulated timeout to mimic that passage of time.
    now = [0.0]
    select_outcomes = iter([True, False, False, True])

    def fake_select(rlist, wlist, xlist, timeout):  # pylint: disable=unused-argument
        if next(select_outcomes):
            return (rlist, [], [])
        now[0] += timeout
        return ([], [], [])

    mocker.patch("rs_dpr_service.dask.call_dask.time.monotonic", side_effect=lambda: now[0])
    mocker.patch("rs_dpr_service.dask.call_dask.select.select", side_effect=fake_select)
    mocker.patch("rs_dpr_service.dask.call_dask.subprocess.Popen", return_value=fake_proc)
    mocker.patch("rs_dpr_service.dask.call_dask.get_client", side_effect=ValueError)
    monkeypatch.setattr(call_dask.settings, "CLUSTER_MODE", False)
    logger_info = mocker.patch("rs_dpr_service.dask.call_dask.logger.info")
    span = mocker.Mock()

    caller._launch_eopf_subprocess(span, {})  # pylint: disable=protected-access

    job_prefix = f"[JOB:{caller.job_id}]"
    all_calls = [call.args[0] for call in logger_info.call_args_list]
    batch_calls = [msg for msg in all_calls if msg.startswith(job_prefix)]
    # Flushed once during the idle period, well before EOF: not held back until the process ends.
    assert batch_calls == ["[JOB:job-1] first line"]


def test_should_flush_log_batch_returns_false_for_an_empty_batch(mocker):
    """Test _should_flush_log_batch() never flushes an empty batch, even long after the interval."""
    caller = _make_processor_caller(mocker)
    mocker.patch("rs_dpr_service.dask.call_dask.time.monotonic", return_value=1000.0)

    assert not caller._should_flush_log_batch([], last_flush=0.0)  # pylint: disable=protected-access


def test_should_flush_log_batch_returns_true_when_the_batch_is_full(mocker):
    """Test _should_flush_log_batch() flushes as soon as the batch reaches its size limit."""
    caller = _make_processor_caller(mocker)
    batch_size = call_dask.ProcessorCaller._LOG_BATCH_SIZE  # pylint: disable=protected-access
    mocker.patch("rs_dpr_service.dask.call_dask.time.monotonic", return_value=0.0)
    full_batch = ["line"] * batch_size

    assert caller._should_flush_log_batch(full_batch, last_flush=0.0)  # pylint: disable=protected-access


def test_should_flush_log_batch_returns_false_before_the_interval_elapses(mocker):
    """Test _should_flush_log_batch() waits for the interval before flushing a partial batch."""
    caller = _make_processor_caller(mocker)
    interval = call_dask.ProcessorCaller._LOG_BATCH_INTERVAL_SECONDS  # pylint: disable=protected-access
    mocker.patch("rs_dpr_service.dask.call_dask.time.monotonic", return_value=interval - 0.1)

    assert not caller._should_flush_log_batch(["line"], last_flush=0.0)  # pylint: disable=protected-access


def test_should_flush_log_batch_returns_true_after_the_interval_elapses(mocker):
    """Test _should_flush_log_batch() flushes a partial batch once it has been idle long enough."""
    caller = _make_processor_caller(mocker)
    interval = call_dask.ProcessorCaller._LOG_BATCH_INTERVAL_SECONDS  # pylint: disable=protected-access
    mocker.patch("rs_dpr_service.dask.call_dask.time.monotonic", return_value=interval)

    assert caller._should_flush_log_batch(["line"], last_flush=0.0)  # pylint: disable=protected-access


def test_launch_eopf_subprocess_raises_and_skips_final_message_on_nonzero_status(mocker, monkeypatch, tmp_path):
    """Test _launch_eopf_subprocess raises when the subprocess exits with a non-zero status."""
    caller = _make_processor_caller(mocker)
    caller.log_path = str(tmp_path / "eopf.log")
    caller.working_dir = str(tmp_path)
    fake_proc = _fake_subprocess(mocker, ["boom\n"], returncode=1)

    mocker.patch("rs_dpr_service.dask.call_dask.subprocess.Popen", return_value=fake_proc)
    mocker.patch("rs_dpr_service.dask.call_dask.get_client", side_effect=ValueError)
    monkeypatch.setattr(call_dask.settings, "CLUSTER_MODE", False)
    span = mocker.Mock()

    with pytest.raises(RuntimeError, match="EOPF error, status code 1"):
        caller._launch_eopf_subprocess(span, {})  # pylint: disable=protected-access


def test_flush_log_batch_logs_joined_lines_and_clears_the_buffer(mocker):
    """Test _flush_log_batch() emits one logger call for the whole batch, then empties it."""
    caller = _make_processor_caller(mocker)
    logger_info = mocker.patch("rs_dpr_service.dask.call_dask.logger.info")
    batch = ["first line", "second line"]

    caller._flush_log_batch(batch)  # pylint: disable=protected-access

    logger_info.assert_called_once_with("[JOB:job-1] first line\nsecond line")
    assert not batch


def test_flush_log_batch_does_nothing_for_an_empty_batch(mocker):
    """Test _flush_log_batch() is a no-op when there is nothing buffered."""
    caller = _make_processor_caller(mocker)
    logger_info = mocker.patch("rs_dpr_service.dask.call_dask.logger.info")

    caller._flush_log_batch([])  # pylint: disable=protected-access

    logger_info.assert_not_called()


# ---- handle_experimental_config ----


def test_handle_experimental_config_uses_lowercase_io_key_fallback(mocker):
    """Test handle_experimental_config() falls back to the 'io' key when 'I/O' is absent."""
    caller = _make_processor_caller(mocker)
    caller.data = {"experimental_config": {"local_files": {"local_dir": "/data/local"}}}
    handle_local_product = mocker.patch.object(caller, "handle_local_product")

    product = {"path": "s3://bucket/product"}
    payload = {"io": {"input_products": [product]}}

    caller.handle_experimental_config(payload)

    handle_local_product.assert_called_once_with("input_products", product)


# ---- _collect_storage_options ----


def test_collect_storage_options_returns_empty_when_no_io_section():
    """Returns an empty list when the payload has no I/O section."""
    assert not call_dask.ProcessorCaller._collect_storage_options({})  # pylint: disable=protected-access


def test_collect_storage_options_gathers_all_sections():
    """Collects storage_options from input_products, output_products, and adfs."""
    so_in = {"key": "k1", "secret": "s1"}  # nosec
    so_out = {"key": "k2", "secret": "s2"}  # nosec
    so_adf = {"key": "k3", "secret": "s3"}  # nosec
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
                            "secret": "s1",  # nosec
                            "client_kwargs": {"endpoint_url": "u1", "region_name": "r1"},
                        },
                    },
                },
                {
                    "reader_params": {
                        "storage_options": {
                            "key": "k2",
                            "secret": "s2",  # nosec
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
        "secret": "mysecret",  # nosec
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
            "secret": "mysecret",  # nosec
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
        "secret": "s",  # nosec
        "client_kwargs": {"endpoint_url": "u", "region_name": "r"},
    }
    payload = {"I/O": {"input_products": [{"reader_params": {"storage_options": creds}}]}}

    caller.write_secret_conf_files(["secrets.json"], payload, str(payload_dir / "payload.yaml"))

    assert (payload_dir / "secrets.json").exists()
    assert not (tmp_path / "secrets.json").exists()
