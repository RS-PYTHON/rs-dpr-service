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

import asyncio
import base64
import importlib
import io
import json
import os
import pickle  # nosec B403 # only used to check that a plugin built by this test is picklable
import re
import sys
import types
import zipfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import cloudpickle
import pytest
from distributed.diagnostics.plugin import SchedulerPlugin, WorkerPlugin
from distributed.scheduler import Scheduler
from distributed.worker import Worker

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


def test_build_module_zip_is_deterministic():
    """Test build_module_zip() names the zip after its content, which only depends on the source code."""
    zip_filename, data = call_dask.build_module_zip()

    assert re.fullmatch(r"rs_dpr_service-[0-9a-f]{16}\.zip", zip_filename)
    assert call_dask.build_module_zip() == (zip_filename, data)
    # The uploaded archive must contain the worker-side code used later by Dask tasks.
    with zipfile.ZipFile(io.BytesIO(data)) as zipped:
        assert "rs_dpr_service/dask/call_dask.py" in zipped.namelist()
        assert "rs_dpr_service/safe_to_zarr.py" in zipped.namelist()
        assert "rs_dpr_service/utils/settings.py" in zipped.namelist()


def test_upload_this_module_registers_idempotent_plugins(mocker):
    """Test upload_this_module() registers picklable idempotent scheduler and worker plugins named after the zip."""
    dask_client = mocker.Mock()
    dask_client.run_on_scheduler.return_value = ([], [])

    call_dask.upload_this_module(dask_client)

    zip_filename, data = call_dask.build_module_zip()
    plugin_name = zip_filename.removesuffix(".zip")
    assert [c.kwargs for c in dask_client.register_plugin.call_args_list] == [{"name": plugin_name}] * 2
    scheduler_plugin, worker_plugin = (c.args[0] for c in dask_client.register_plugin.call_args_list)
    # The scheduler plugin is registered first: the scheduler unpickles the task graphs
    assert isinstance(scheduler_plugin, SchedulerPlugin)
    assert isinstance(worker_plugin, WorkerPlugin)
    plugins: list[Any] = [scheduler_plugin, worker_plugin]  # attributes of the plugin classes defined in call_dask
    for plugin in plugins:
        assert plugin.idempotent is True
        assert plugin.zip_filename == zip_filename
        assert plugin.data == data
        assert plugin.module_names[0] == "rs_dpr_service"
        assert "rs_dpr_service.dask.call_dask" in plugin.module_names
        # The dask scheduler and workers don't have this module before the plugin installs it: the plugin class
        # must not be pickled by reference (standard pickle), but by value (cloudpickle, used by dask as fallback)
        with pytest.raises((pickle.PicklingError, AttributeError)):
            pickle.dumps(plugin)
        assert pickle.loads(cloudpickle.dumps(plugin)).zip_filename == zip_filename  # nosec B301


def test_upload_this_module_unregisters_stale_plugins(mocker):
    """Test upload_this_module() unregisters the plugins of the other versions of this module, only."""
    zip_filename, _ = call_dask.build_module_zip()
    plugin_name = zip_filename.removesuffix(".zip")
    legacy = "/scratch/tmpqe6tlbfv/rs_dpr_service.zip92b8a41d-f7c3-4bcd-ab93-d3a08daf7590"
    already_removed = "/scratch/tmpine3w56_/rs_dpr_service.zip74c4c37e-53d1-456c-b353-be83b9d1be19"
    previous = "rs_dpr_service-0123456789abcdef"
    others = ["shuffle", "forward-logging-<root>", "other_package-0123456789abcdef", plugin_name]
    dask_client = mocker.Mock()
    dask_client.run_on_scheduler.return_value = (
        ["stealing", legacy, previous, plugin_name],
        [*others, legacy, already_removed, previous],
    )

    def unregister(name):
        if name == already_removed:
            raise ValueError(f"The worker plugin {name} does not exist")

    dask_client.unregister_worker_plugin.side_effect = unregister

    call_dask.upload_this_module(dask_client)

    dask_client.run_on_scheduler.assert_called_once()
    plugin_names = dask_client.run_on_scheduler.call_args.args[0]
    dask_scheduler = mocker.Mock(plugins={"a": 1}, worker_plugins={"b": 2, "c": 3})
    assert plugin_names(dask_scheduler) == (["a"], ["b", "c"])
    # The scheduler may not have this module yet: the function must be pickled by value
    with pytest.raises((pickle.PicklingError, AttributeError)):
        pickle.dumps(plugin_names)
    assert [c.args[0] for c in dask_client.unregister_scheduler_plugin.call_args_list] == [legacy, previous]
    assert [c.args[0] for c in dask_client.unregister_worker_plugin.call_args_list] == [
        legacy,
        already_removed,
        previous,
    ]
    assert dask_client.register_plugin.call_count == 2


def _fake_package_zip(version: str) -> bytes:
    """Return the zip of a fake package used to test the worker plugin."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zipped:
        zipped.writestr("fakepkg/__init__.py", "")
        zipped.writestr("fakepkg/mod.py", f"VERSION = {version!r}\n")
    return buffer.getvalue()


@pytest.mark.parametrize("plugin_kind", ["scheduler", "worker"])
def test_module_install_plugins(monkeypatch, tmp_path, plugin_kind):
    """Test the plugins install the zip atomically and only reload the modules loaded from another file."""
    monkeypatch.setattr(sys, "path", list(sys.path))
    for name in ("fakepkg", "fakepkg.mod"):
        monkeypatch.delitem(sys.modules, name, raising=False)
    worker_dir = tmp_path / "worker"
    worker_dir.mkdir()
    node = SimpleNamespace(local_directory=str(worker_dir))
    module_names = ["fakepkg", "fakepkg.mod"]

    def install(version: str):
        scheduler_plugin, worker_plugin = call_dask.make_module_install_plugins(
            f"fakepkg-{version}.zip",
            _fake_package_zip(version),
            module_names,
        )
        if plugin_kind == "scheduler":
            asyncio.run(scheduler_plugin.start(cast(Scheduler, node)))
        else:
            worker_plugin.setup(cast(Worker, node))

    # A previous version of the package is already loaded on the worker
    previous = tmp_path / "previous"
    (previous / "fakepkg").mkdir(parents=True)
    (previous / "fakepkg/__init__.py").write_text("", encoding="utf-8")
    (previous / "fakepkg/mod.py").write_text("VERSION = 'v1'\n", encoding="utf-8")
    sys.path.insert(0, str(previous))
    fake_mod = importlib.import_module("fakepkg.mod")
    assert fake_mod.VERSION == "v1"

    # Install the new version: the modules are reloaded from the zip
    install("v2")
    zip_path = worker_dir / "fakepkg-v2.zip"
    assert zip_path.is_file()
    assert not list(worker_dir.glob("*.tmp"))
    assert sys.path[0] == str(zip_path)
    assert sys.modules["fakepkg.mod"].VERSION == "v2"
    assert str(sys.modules["fakepkg.mod"].__file__).startswith(str(zip_path))

    # Another job installs the same version: the zip is not rewritten and the modules are not reloaded
    vars(sys.modules["fakepkg.mod"])["MARKER"] = "not reloaded"
    inode = zip_path.stat().st_ino
    install("v2")
    assert zip_path.stat().st_ino == inode
    assert vars(sys.modules["fakepkg.mod"])["MARKER"] == "not reloaded"
    assert sys.path.count(str(zip_path)) == 1


@pytest.fixture(name="conversion_zip")
def fixture_conversion_zip(monkeypatch, tmp_path):
    """Fake the uploaded worker zip that convert_safe_to_zarr() derives from call_dask.__file__."""
    zip_path = tmp_path / "rs_dpr_service.zip"
    zip_path.write_text("fake zip content", encoding="utf-8")
    monkeypatch.setattr(
        call_dask,
        "__file__",
        str(zip_path / "rs_dpr_service" / "dask" / "call_dask.py"),
    )
    return zip_path


def _mock_conversion_subprocess(mocker, lines: list[str], returncode: int = 0, result: str | None = None):
    """Mock the conversion subprocess: it outputs the given lines, writes the result pipe, exits with returncode."""
    fake_proc = _fake_subprocess(mocker, lines, returncode)

    def popen(cmd, **kwargs):
        # The result is written into the inherited write end of the result pipe
        assert kwargs["pass_fds"] == (int(cmd[4]),)
        if result is not None:
            os.write(int(cmd[4]), result.encode())
        return fake_proc

    mocker.patch("rs_dpr_service.dask.call_dask.log_worker_resources")
    return mocker.patch("rs_dpr_service.dask.call_dask.subprocess.Popen", side_effect=popen)


def test_convert_safe_to_zarr_runs_safe_to_zarr_module(mocker, monkeypatch, conversion_zip):
    """Test convert_safe_to_zarr() calls the safe_to_zarr module from the uploaded worker zip."""
    popen = _mock_conversion_subprocess(mocker, ["eopf log line\n"], result='{"message": "Conversion finished"}\n')
    logger_info = mocker.patch("rs_dpr_service.dask.call_dask.logger.info")
    cfg = {"safe_uri": "s3://bucket/input.SAFE", "zarr_uri": "s3://bucket/output.zarr"}

    # The S3 credentials are not in the dask worker environment
    for key in call_dask.S3_ENV_VARS:
        monkeypatch.delenv(key, raising=False)
    s3_env = {key: f"{key.lower()}-value" for key in call_dask.S3_ENV_VARS}

    result = call_dask.convert_safe_to_zarr(cfg, s3_env, "job-1")

    # The result is read from the result pipe, not from the subprocess output
    assert result == '{"message": "Conversion finished"}'
    popen.assert_called_once()
    command = popen.call_args.args[0]
    env = popen.call_args.kwargs["env"]
    assert command[:3] == [call_dask.sys.executable, "-m", "rs_dpr_service.safe_to_zarr"]
    assert json.loads(command[3]) == cfg
    assert env["PYTHONPATH"].split(os.pathsep)[0] == str(conversion_zip)
    # The S3 credentials of the calling service are passed to the conversion subprocess
    assert {key: env[key] for key in call_dask.S3_ENV_VARS} == s3_env
    # The output (stdout and stderr) is forwarded live to the job log stream
    assert popen.call_args.kwargs["stderr"] == call_dask.subprocess.STDOUT
    logger_info.assert_any_call("[JOB:job-1] eopf log line", extra={call_dask.EOPF_OUTPUT_LOG_ATTR: True})


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


@pytest.mark.parametrize(
    "returncode, expected",
    [
        (1, "Conversion failed: exit code 1. Last 2 output lines:\nTraceback...\nConversion failed safe_to_zarr: boom"),
        (
            -9,
            "Conversion failed: killed by SIGKILL (exit code -9): probably out of memory",
        ),
    ],
)
def test_convert_safe_to_zarr_raises_when_subprocess_fails(mocker, conversion_zip, returncode, expected):
    """Test convert_safe_to_zarr() explains the exit code and repeats the last output lines on failure."""
    assert conversion_zip.is_file()
    _mock_conversion_subprocess(
        mocker,
        ["Traceback...\n", "Conversion failed safe_to_zarr: boom\n"],
        returncode=returncode,
    )
    mocker.patch("rs_dpr_service.dask.call_dask.logger.info")

    with pytest.raises(RuntimeError) as error:
        call_dask.convert_safe_to_zarr({"safe_uri": "input.SAFE"})
    assert str(error.value).startswith(expected)
    assert str(error.value).endswith("Conversion failed safe_to_zarr: boom")


def test_convert_safe_to_zarr_raises_when_no_result_is_written(mocker, conversion_zip):
    """Test convert_safe_to_zarr() fails when the subprocess succeeds without writing its result."""
    assert conversion_zip.is_file()
    _mock_conversion_subprocess(mocker, [], returncode=0, result=None)
    mocker.patch("rs_dpr_service.dask.call_dask.logger.info")

    with pytest.raises(RuntimeError, match="Conversion failed: no result written. No output."):
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


@pytest.fixture(name="init_context")
def fixture_init_context(mocker, monkeypatch, tmp_path):
    """Prepare real configuration files and stub only worker storage and payload customization."""
    caller = _make_processor_caller(mocker)
    config_dir = tmp_path / "config"
    payload_dir = config_dir / "nested"
    payload_dir.mkdir(parents=True)
    payload_file = payload_dir / "payload.yaml"
    payload_file.write_text("workflow: []\n", encoding="utf-8")
    reports_dir = payload_dir / "reports"
    reports_dir.mkdir()
    (reports_dir / "old.processor.log").write_text("old report", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    for key in ("S3_ACCESSKEY", "S3_SECRETKEY", "S3_ENDPOINT", "S3_REGION"):
        monkeypatch.setenv(key, caller.caller_env[key])

    anypath_init = mocker.Mock()
    download = mocker.Mock(return_value=str(config_dir))

    class FakeAnyPath:
        """Model the worker's AnyPath type and its download method."""

        def __init__(self, *args, **kwargs):
            anypath_init(*args, **kwargs)

        def get(self, **kwargs):
            """Return the configured download result without contacting S3."""
            return download(**kwargs)

    file_utils = types.ModuleType("eopf.common.file_utils")
    monkeypatch.setattr(file_utils, "AnyPath", FakeAnyPath, raising=False)
    eopf_module = types.ModuleType("eopf")
    # Older than 3.1.0rc1: the payload validation is skipped
    monkeypatch.setattr(eopf_module, "__version__", "3.0.0", raising=False)
    mocker.patch.dict(
        sys.modules,
        {
            "eopf": eopf_module,
            "eopf.common": types.ModuleType("eopf.common"),
            "eopf.common.file_utils": file_utils,
        },
    )

    def customize(payload_path):
        """Ensure initialization prepares the filesystem before payload customization."""
        assert payload_path == str(payload_file)
        assert Path.cwd() == payload_dir
        assert reports_dir.is_dir()
        assert not list(reports_dir.iterdir())

    customize_payload = mocker.patch.object(caller, "customize_payload_file", side_effect=customize)
    caller.data = {
        "s3_config_dir": str(config_dir),
        "payload_subpath": "nested/../nested/payload.yaml",
        "s3_report_dir": "s3://bucket/job-reports",
    }
    return SimpleNamespace(
        caller=caller,
        config_dir=config_dir,
        payload_dir=payload_dir,
        payload_file=payload_file,
        reports_dir=reports_dir,
        eopf_module=eopf_module,
        anypath_class=FakeAnyPath,
        anypath_init=anypath_init,
        download=download,
        customize_payload=customize_payload,
    )


@pytest.mark.parametrize(
    "config_source",
    ["local-absolute", "local-relative", "s3-string", "s3-fs-path", "s3-legacy-path", "s3-uppercase"],
)
def test_processor_caller_init_prepares_payload_and_reports(init_context, config_source):
    """Initialize local configurations and all supported EOPF download return formats."""
    ctx = init_context
    caller = ctx.caller
    is_s3 = config_source.startswith("s3")
    if config_source == "local-relative":
        caller.data["s3_config_dir"] = "config"
    elif is_s3:
        caller.data["s3_config_dir"] = "S3://bucket/config" if config_source == "s3-uppercase" else "s3://bucket/config"
        if config_source in ("s3-fs-path", "s3-legacy-path"):
            downloaded_dir = object.__new__(ctx.anypath_class)
            if config_source == "s3-fs-path":
                downloaded_dir.fs_path = str(ctx.config_dir)
                downloaded_dir.path = "must-not-use-legacy-path"
            else:
                downloaded_dir.path = str(ctx.config_dir)
            ctx.download.return_value = downloaded_dir
    caller.s3 = object()

    caller.init()

    if is_s3:
        ctx.anypath_init.assert_called_once_with(
            caller.data["s3_config_dir"],
            key=caller.caller_env["S3_ACCESSKEY"],
            secret=caller.caller_env["S3_SECRETKEY"],
            client_kwargs={
                "endpoint_url": caller.caller_env["S3_ENDPOINT"],
                "region_name": caller.caller_env["S3_REGION"],
            },
        )
        ctx.download.assert_called_once_with(recursive=True)
        assert isinstance(caller.s3, ctx.anypath_class)
    else:
        ctx.anypath_init.assert_not_called()
        ctx.download.assert_not_called()
        assert caller.s3 is None
    assert caller.s3_report_dir == "s3://bucket/job-reports"
    assert caller.working_dir == str(ctx.payload_dir)
    assert caller.local_report_dir == str(ctx.reports_dir)
    assert caller.log_path == str(ctx.reports_dir / "payload.processor.log")
    ctx.customize_payload.assert_called_once_with(str(ctx.payload_file))
    assert caller.command == ["eopf_otel", "trigger", "local", str(ctx.payload_file)]
    assert ctx.payload_file.read_text(encoding="utf-8") == "workflow: []\n"


def test_processor_caller_init_validates_payload_with_recent_eopf(init_context, mocker):
    """The payload is validated at the preflight level when eopf is 3.1.0rc1 or newer."""
    ctx = init_context
    ctx.eopf_module.__version__ = "3.1.0"
    validate_payload_command = mocker.Mock()
    triggers = types.ModuleType("eopf.cli.cli_triggering_triggers")
    mocker.patch.object(triggers, "validate_payload_command", validate_payload_command, create=True)
    mocker.patch.dict(
        sys.modules,
        {"eopf.cli": types.ModuleType("eopf.cli"), "eopf.cli.cli_triggering_triggers": triggers},
    )
    ctx.caller.s3 = object()

    ctx.caller.init()

    validate_payload_command.main.assert_called_once_with(
        args=[str(ctx.payload_file), "--level", "preflight"],
        standalone_mode=False,
    )


def test_processor_caller_init_propagates_download_error(init_context):
    """Abort initialization without changing directories or reports when downloading fails."""
    ctx = init_context
    ctx.caller.data["s3_config_dir"] = "s3://bucket/config"
    ctx.download.side_effect = OSError("configuration download failed")
    original_dir = Path.cwd()

    with pytest.raises(OSError, match="configuration download failed"):
        ctx.caller.init()

    assert Path.cwd() == original_dir
    assert (ctx.reports_dir / "old.processor.log").read_text(encoding="utf-8") == "old report"
    ctx.customize_payload.assert_not_called()
    assert ctx.caller.command == []


def test_processor_caller_init_propagates_customization_error(init_context):
    """Do not prepare a processor command if payload customization fails."""
    ctx = init_context
    ctx.customize_payload.side_effect = ValueError("invalid payload")

    with pytest.raises(ValueError, match="invalid payload"):
        ctx.caller.init()

    ctx.customize_payload.assert_called_once_with(str(ctx.payload_file))
    assert ctx.caller.command == []


def test_processor_caller_run_processor_runs_nominal_orchestration(mocker, monkeypatch):
    """Test ProcessorCaller.run_processor() copies env, runs init/trigger/finalize, and returns the result."""
    # Track env restoration even though copy_caller_env() writes directly into os.environ.
    for key in (
        "S3_ACCESSKEY",
        "S3_SECRETKEY",
        "S3_ENDPOINT",
        "S3_REGION",
        "S3_SECRETS_JSON_BASE64",
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
    assert os.environ["S3_SECRETS_JSON_BASE64"] == ""
    assert os.environ["OTEL_RESOURCE_ATTRIBUTES"] == "service.name=test"
    assert caller.exec_times[0][0] == "Run processor"


@pytest.mark.parametrize(
    "endpoint, previous_allow_http, allow_http",
    [
        ("http://seaweedfs-s3:9000", None, "true"),
        # Value left by a previous job with an http endpoint must be removed
        ("https://s3.test", "true", None),
    ],
)
def test_processor_caller_set_aws_env_allows_http_endpoints(
    mocker,
    monkeypatch,
    endpoint,
    previous_allow_http,
    allow_http,
):
    """Test ProcessorCaller.set_aws_env() allows http endpoints for the EOPF obstore uploads."""
    monkeypatch.setenv("S3_ACCESSKEY", "access-key")
    monkeypatch.setenv("S3_SECRETKEY", "secret-key")
    monkeypatch.setenv("S3_ENDPOINT", endpoint)
    monkeypatch.setenv("S3_REGION", "eu-west-1")
    if previous_allow_http:
        monkeypatch.setenv("AWS_ALLOW_HTTP", previous_allow_http)
    else:
        monkeypatch.delenv("AWS_ALLOW_HTTP", raising=False)
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_ENDPOINT_URL_S3", "AWS_DEFAULT_REGION"):
        monkeypatch.setenv(key, "")
    monkeypatch.delenv("AWS_DEFAULT_OUTPUT", raising=False)

    _make_processor_caller(mocker).set_aws_env()

    assert os.environ["AWS_ENDPOINT_URL_S3"] == endpoint
    assert os.environ.get("AWS_ALLOW_HTTP") == allow_http


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


# ---- _launch_eopf_subprocess / forward_subprocess_output / _flush_log_batch ----


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
    batch_size = call_dask.LOG_BATCH_SIZE

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
        "[JOB:job-1] EOPF finished successfully with status code 0",
    ]
    # Only the raw EOPF output records are flagged, so the log stream does not add a prefix to them
    batch_extras = [
        call.kwargs.get("extra") for call in logger_info.call_args_list if call.args[0].startswith(job_prefix)
    ]
    assert batch_extras == [{call_dask.EOPF_OUTPUT_LOG_ATTR: True}, {call_dask.EOPF_OUTPUT_LOG_ATTR: True}, None]

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
    assert batch_calls == ["[JOB:job-1] first line", "[JOB:job-1] EOPF finished successfully with status code 0"]


def test_should_flush_log_batch_returns_false_for_an_empty_batch(mocker):
    """Test _should_flush_log_batch() never flushes an empty batch, even long after the interval."""
    mocker.patch("rs_dpr_service.dask.call_dask.time.monotonic", return_value=1000.0)

    assert not call_dask._should_flush_log_batch([], last_flush=0.0)  # pylint: disable=protected-access


def test_should_flush_log_batch_returns_true_when_the_batch_is_full(mocker):
    """Test _should_flush_log_batch() flushes as soon as the batch reaches its size limit."""
    batch_size = call_dask.LOG_BATCH_SIZE
    mocker.patch("rs_dpr_service.dask.call_dask.time.monotonic", return_value=0.0)
    full_batch = ["line"] * batch_size

    assert call_dask._should_flush_log_batch(full_batch, last_flush=0.0)  # pylint: disable=protected-access


def test_should_flush_log_batch_returns_false_before_the_interval_elapses(mocker):
    """Test _should_flush_log_batch() waits for the interval before flushing a partial batch."""
    interval = call_dask.LOG_BATCH_INTERVAL_SECONDS
    mocker.patch("rs_dpr_service.dask.call_dask.time.monotonic", return_value=interval - 0.1)

    assert not call_dask._should_flush_log_batch(["line"], last_flush=0.0)  # pylint: disable=protected-access


def test_should_flush_log_batch_returns_true_after_the_interval_elapses(mocker):
    """Test _should_flush_log_batch() flushes a partial batch once it has been idle long enough."""
    interval = call_dask.LOG_BATCH_INTERVAL_SECONDS
    mocker.patch("rs_dpr_service.dask.call_dask.time.monotonic", return_value=interval)

    assert call_dask._should_flush_log_batch(["line"], last_flush=0.0)  # pylint: disable=protected-access


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

    # The error message repeats the last output lines, so the job status alone tells what went wrong
    with pytest.raises(RuntimeError, match="EOPF error, exit code 1, please see the log. Last 1 output lines:\nboom"):
        caller._launch_eopf_subprocess(span, {})  # pylint: disable=protected-access


@pytest.mark.parametrize(
    "cancelled, expected",
    [
        (False, "EOPF error, killed by SIGKILL (exit code -9): probably out of memory"),
        (True, "EOPF error, cancelled by the user, please see the log."),
    ],
)
def test_launch_eopf_subprocess_explains_a_killed_subprocess(mocker, monkeypatch, tmp_path, cancelled, expected):
    """Test _launch_eopf_subprocess explains a subprocess killed by a signal, except on a job cancellation."""
    caller = _make_processor_caller(mocker)
    caller.log_path = str(tmp_path / "eopf.log")
    caller.working_dir = str(tmp_path)
    caller.cancelled = cancelled
    fake_proc = _fake_subprocess(mocker, ["processing...\n"], returncode=-9)

    mocker.patch("rs_dpr_service.dask.call_dask.subprocess.Popen", return_value=fake_proc)
    mocker.patch("rs_dpr_service.dask.call_dask.get_client", side_effect=ValueError)
    monkeypatch.setattr(call_dask.settings, "CLUSTER_MODE", False)

    span = mocker.Mock()

    with pytest.raises(RuntimeError) as error:
        caller._launch_eopf_subprocess(span, {})  # pylint: disable=protected-access
    assert str(error.value).startswith(expected)
    assert str(error.value).endswith("Last 1 output lines:\nprocessing...")


def test_forward_subprocess_output_returns_the_last_lines(mocker):
    """Test forward_subprocess_output() returns only the LOG_TAIL_SIZE last non-empty lines."""
    lines = [f"line {i}\n" for i in range(call_dask.LOG_TAIL_SIZE + 5)] + ["\n"]
    fake_proc = _fake_subprocess(mocker, lines)
    mocker.patch("rs_dpr_service.dask.call_dask.logger.info")

    tail = call_dask.forward_subprocess_output(fake_proc, "job-1")

    assert list(tail) == [f"line {i}" for i in range(5, call_dask.LOG_TAIL_SIZE + 5)]


@pytest.mark.parametrize(
    "returncode, expected",
    [
        (1, "exit code 1"),
        (-9, "killed by SIGKILL (exit code -9): probably out of memory"),
        (137, "killed by SIGKILL (exit code 137): probably out of memory"),
        (-15, "killed by SIGTERM (exit code -15): terminated from outside"),
        (-11, "killed by SIGSEGV (exit code -11): segmentation fault"),
        (-2, "killed by SIGINT (exit code -2)"),
        (-100, "killed by signal 100 (exit code -100)"),
    ],
)
def test_describe_exit_code(returncode, expected):
    """Test describe_exit_code() explains the subprocess deaths by signal."""
    assert call_dask.describe_exit_code(returncode).startswith(expected)


def test_format_output_tail():
    """Test format_output_tail() formats the last output lines appended to the error messages."""
    assert call_dask.format_output_tail([]) == " No output."
    assert call_dask.format_output_tail(["a", "b"]) == " Last 2 output lines:\na\nb"


@pytest.mark.parametrize(
    "files, expected",
    [
        ({"memory.max": "8589934592\n"}, 8589934592),
        ({"memory.max": "max\n"}, None),
        ({"memory.limit_in_bytes": "4294967296\n"}, 4294967296),
        ({"memory.limit_in_bytes": "9223372036854771712\n"}, None),  # cgroup v1 "no limit"
        ({"memory.max": "invalid\n"}, None),
        ({}, None),
    ],
)
def test_get_memory_limit_reads_the_cgroup_files(monkeypatch, tmp_path, files, expected):
    """Test get_memory_limit() reads the cgroup v2, then v1, memory limit."""
    for name, value in files.items():
        (tmp_path / name).write_text(value, encoding="utf-8")
    monkeypatch.setattr(
        call_dask,
        "CGROUP_MEMORY_LIMIT_FILES",
        (str(tmp_path / "memory.max"), str(tmp_path / "memory.limit_in_bytes")),
    )

    assert call_dask.get_memory_limit() == expected


def test_log_worker_resources_logs_memory_and_disk(mocker, tmp_path):
    """Test log_worker_resources() logs the worker memory limit and free disk space."""
    mocker.patch("rs_dpr_service.dask.call_dask.get_memory_limit", return_value=8 * 1024**3)
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="10.0.0.1")
    mocker.patch(
        "rs_dpr_service.dask.call_dask.shutil.disk_usage",
        return_value=SimpleNamespace(free=20 * 1024**3, total=100 * 1024**3),
    )
    logger_info = mocker.patch("rs_dpr_service.dask.call_dask.logger.info")

    call_dask.log_worker_resources("job-1", str(tmp_path))

    logger_info.assert_called_once_with(
        f"[JOB:job-1] Dask worker '10.0.0.1' resources: container memory limit: 8.0 GiB, "
        f"free disk space in {str(tmp_path)!r}: 20.0 GiB / 100.0 GiB",
    )


def test_log_worker_resources_never_fails(mocker, tmp_path):
    """Test log_worker_resources() only logs a debug message when the resources cannot be read."""
    mocker.patch("rs_dpr_service.dask.call_dask.get_memory_limit", return_value=None)
    mocker.patch("rs_dpr_service.dask.call_dask.shutil.disk_usage", side_effect=OSError("no disk"))
    logger_debug = mocker.patch("rs_dpr_service.dask.call_dask.logger.debug")

    call_dask.log_worker_resources("job-1", str(tmp_path))

    logger_debug.assert_called_once_with("[JOB:job-1] Cannot read the dask worker resources: no disk")


def test_format_bytes():
    """Test format_bytes() returns human readable sizes."""
    assert call_dask.format_bytes(512) == "512.0 B"
    assert call_dask.format_bytes(1536) == "1.5 KiB"
    assert call_dask.format_bytes(9160613174) == "8.5 GiB"
    assert call_dask.format_bytes(3 * 1024**4) == "3.0 TiB"


def test_flush_log_batch_logs_joined_lines_and_clears_the_buffer(mocker):
    """Test _flush_log_batch() emits one logger call for the whole batch, then empties it."""
    logger_info = mocker.patch("rs_dpr_service.dask.call_dask.logger.info")
    batch = ["first line", "second line"]

    call_dask._flush_log_batch("job-1", batch)  # pylint: disable=protected-access

    logger_info.assert_called_once_with(
        "[JOB:job-1] first line\nsecond line",
        extra={call_dask.EOPF_OUTPUT_LOG_ATTR: True},
    )
    assert not batch


def test_flush_log_batch_does_nothing_for_an_empty_batch(mocker):
    """Test _flush_log_batch() is a no-op when there is nothing buffered."""
    logger_info = mocker.patch("rs_dpr_service.dask.call_dask.logger.info")

    call_dask._flush_log_batch("job-1", [])  # pylint: disable=protected-access

    logger_info.assert_not_called()


# ---- handle_local_product ----


@pytest.fixture(name="local_product_context")
def fixture_local_product_context(mocker, tmp_path):
    """Stub worker-only dependencies while keeping local filesystem operations real."""
    caller = _make_processor_caller(mocker)
    caller.experimental_config = call_dask.ExperimentalConfig(
        local_files=call_dask.ExperimentalConfig.LocalFiles(local_dir=str(tmp_path)),
    )
    filesystem = mocker.Mock()
    filesystem_class = mocker.patch("s3fs.S3FileSystem", return_value=filesystem)
    resolve_env_vars = mocker.Mock(side_effect=lambda product: product.copy())
    env_utils = types.ModuleType("eopf.common.env_utils")
    setattr(env_utils, "resolve_env_vars", resolve_env_vars)
    mocker.patch.dict(
        sys.modules,
        {
            "eopf": types.ModuleType("eopf"),
            "eopf.common": types.ModuleType("eopf.common"),
            "eopf.common.env_utils": env_utils,
        },
    )
    return caller, filesystem_class, resolve_env_vars


def _create_local_product(path, kind):
    """Create existing product variants, including links whose targets must survive."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if kind == "file":
        path.write_text("existing product", encoding="utf-8")
    elif kind == "directory":
        path.mkdir()
        (path / "chunk").write_text("existing chunk", encoding="utf-8")
    elif kind in ("symlink", "broken-symlink"):
        target = path.with_name("link-target")
        if kind == "symlink":
            target.write_text("link target", encoding="utf-8")
        path.symlink_to(target)


def test_handle_local_product_without_experimental_config(local_product_context):
    """Leave the product untouched when experimental configuration is disabled."""
    caller, filesystem_class, resolve_env_vars = local_product_context
    caller.experimental_config = None
    product = {"path": "s3://bucket/product"}

    caller.handle_local_product("input_products", product)

    assert product == {"path": "s3://bucket/product"}
    resolve_env_vars.assert_not_called()
    filesystem_class.assert_not_called()
    assert caller.to_be_uploaded == []


@pytest.mark.parametrize("product", [{}, {"path": None}, {"path": ""}, {"path": "/local/product"}])
def test_handle_local_product_skips_missing_or_local_paths(local_product_context, product):
    """Do not access S3 or rewrite products with missing or already-local paths."""
    caller, filesystem_class, resolve_env_vars = local_product_context
    original = product.copy()

    caller.handle_local_product("input_products", product)

    resolve_env_vars.assert_called_once_with(original)
    filesystem_class.assert_not_called()
    assert product == original
    assert caller.to_be_uploaded == []


@pytest.mark.parametrize("io_key", ["input_products", "adfs"])
@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("kind", ["absent", "file", "directory", "symlink", "broken-symlink"])
def test_handle_local_product_downloads_inputs(local_product_context, tmp_path, io_key, overwrite, kind):
    """Reuse existing inputs unless overwrite is enabled, and download missing inputs."""
    caller, filesystem_class, _ = local_product_context
    caller.experimental_config.local_files.overwrite_input = overwrite
    local_path = tmp_path / "bucket" / "nested" / "product"
    _create_local_product(local_path, kind)
    storage_options = {"anon": True}
    product = {
        "path": "s3://bucket/nested/product",
        "store_params": {"storage_options": storage_options},
    }
    should_download = overwrite or kind in ("absent", "broken-symlink")

    def download(s3_path, destination, recursive):
        """Check removal and parent creation before simulating an S3 download."""
        assert s3_path == "s3://bucket/nested/product"
        assert destination == local_path
        assert recursive is True
        assert destination.parent.is_dir()
        assert not destination.exists()
        if overwrite:
            assert not destination.is_symlink()

    filesystem = filesystem_class.return_value
    filesystem.get.side_effect = download

    caller.handle_local_product(io_key, product)

    filesystem_class.assert_called_once_with(**storage_options)
    if should_download:
        filesystem.get.assert_called_once_with("s3://bucket/nested/product", local_path, recursive=True)
    else:
        filesystem.get.assert_not_called()
        assert local_path.exists()
    if kind == "symlink":
        assert local_path.with_name("link-target").read_text(encoding="utf-8") == "link target"
    assert product == {"path": str(local_path), "store_params": {"storage_options": storage_options}}
    assert caller.to_be_uploaded == []


@pytest.mark.parametrize("upload", [False, True])
@pytest.mark.parametrize("kind", ["absent", "file", "directory", "symlink", "broken-symlink"])
def test_handle_local_product_removes_outputs_and_schedules_upload(local_product_context, tmp_path, upload, kind):
    """Always remove old outputs and only queue uploads when explicitly enabled."""
    caller, filesystem_class, _ = local_product_context
    caller.experimental_config.local_files.upload_output = upload
    local_path = tmp_path / "bucket" / "product"
    _create_local_product(local_path, kind)
    product = {"path": "s3://bucket/product", "store_params": {"storage_options": {"anon": True}}}

    caller.handle_local_product("output_products", product)

    filesystem_class.assert_called_once_with(anon=True)
    filesystem_class.return_value.get.assert_not_called()
    assert not local_path.exists()
    assert not local_path.is_symlink()
    if kind == "symlink":
        assert local_path.with_name("link-target").read_text(encoding="utf-8") == "link target"
    assert product["path"] == str(local_path)
    expected_uploads = [(filesystem_class.return_value, str(local_path), "s3://bucket/product")] if upload else []
    assert caller.to_be_uploaded == expected_uploads


def test_handle_local_product_rejects_secret_alias(local_product_context):
    """Report unsupported secret aliases before creating an S3 client or changing paths."""
    caller, filesystem_class, _ = local_product_context
    # This is a dummy secret alias, not a credential.
    product = {
        "path": "s3://bucket/product",
        "store_params": {"s3_secret_alias": "bucket-secret"},  # nosec B105
    }

    with pytest.raises(RuntimeError, match="handle bucket credentials from a 'secrets.json' file"):
        caller.handle_local_product("input_products", product)

    filesystem_class.assert_not_called()
    assert product["path"] == "s3://bucket/product"
    assert caller.to_be_uploaded == []


@pytest.mark.parametrize("s3_path", ["s3://bucket/product", "S3://bucket/product", "s3:/bucket/product"])
def test_handle_local_product_uses_resolved_environment(local_product_context, tmp_path, s3_path):
    """Resolve paths and credentials without replacing the original store parameters."""
    caller, filesystem_class, resolve_env_vars = local_product_context
    product = {
        "path": "${PRODUCT_PATH}",
        "store_params": {"storage_options": {"endpoint_url": "${S3_ENDPOINT}"}},
    }
    resolve_env_vars.side_effect = None
    resolve_env_vars.return_value = {
        "path": s3_path,
        "store_params": {"storage_options": {"endpoint_url": "https://s3.test"}},
    }

    caller.handle_local_product("input_products", product)

    resolve_env_vars.assert_called_once_with(product)
    filesystem_class.assert_called_once_with(endpoint_url="https://s3.test")
    filesystem_class.return_value.get.assert_called_once_with(
        s3_path,
        tmp_path / "bucket" / "product",
        recursive=True,
    )
    assert product == {
        "path": str(tmp_path / "bucket" / "product"),
        "store_params": {"storage_options": {"endpoint_url": "${S3_ENDPOINT}"}},
    }


def test_handle_local_product_propagates_download_error(local_product_context):
    """Do not rewrite the payload as if downloading had succeeded when S3 fails."""
    caller, filesystem_class, _ = local_product_context
    filesystem_class.return_value.get.side_effect = OSError("download failed")
    product = {"path": "s3://bucket/product", "store_params": {"storage_options": {}}}

    with pytest.raises(OSError, match="download failed"):
        caller.handle_local_product("input_products", product)

    assert product["path"] == "s3://bucket/product"
    assert caller.to_be_uploaded == []


# ---- _collect_storage_options ----


def test_collect_storage_options_returns_empty_when_no_io_section():
    """Returns an empty list when the payload has no io section."""
    assert not call_dask.ProcessorCaller._collect_storage_options({})  # pylint: disable=protected-access


def test_collect_storage_options_gathers_all_sections():
    """Collects storage_options from input_products, output_products, and adfs."""
    so_in = {"key": "k1", "secret": "s1"}  # nosec
    so_out = {"key": "k2", "secret": "s2"}  # nosec
    so_adf = {"key": "k3", "secret": "s3"}  # nosec
    payload = {
        "io": {
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
        "io": {
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
        "io": {
            "input_products": [{"store_params": {"storage_options": so}}],
            "output_products": [{"store_params": {"storage_options": so}}],
            "adfs": [{"store_params": {"storage_options": so}}],
        },
    }
    result = call_dask.ProcessorCaller._collect_storage_options(payload)  # pylint: disable=protected-access
    assert result == [so, so, so]


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
        "io": {
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
        "io": {
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
    payload = {"io": {"input_products": [{"reader_params": {"storage_options": creds}}]}}

    caller.write_secret_conf_files(["secrets.json"], payload, str(payload_dir / "payload.yaml"))

    assert (payload_dir / "secrets.json").exists()
    assert not (tmp_path / "secrets.json").exists()


@pytest.mark.parametrize("params_name", ["reader_params", "store_params"])
def test_payload_s3_secrets_uses_input_credentials(monkeypatch, params_name):
    """The s3input alias uses payload credentials, independent of host credentials."""
    monkeypatch.setenv("S3_ACCESSKEY", "host-key")
    monkeypatch.setenv("S3_SECRETS_JSON_BASE64", "stale")
    # Registered so that the variables set by set_payload_s3_secrets are restored after the test
    for key in ("S3_KEY", "S3_SECRET", "S3_URL", "S3_REGION"):
        monkeypatch.delenv(key, raising=False)
    credential = {
        "key": "payload-key",
        "secret": "payload-secret",  # nosec B105
        "endpoint_url": "https://payload.test",
        "region_name": "gra",
    }
    options = {
        "key": credential["key"],
        "secret": credential["secret"],
        "client_kwargs": {"endpoint_url": credential["endpoint_url"], "region_name": credential["region_name"]},
    }
    payload = {"io": {"input_products": [{params_name: {"storage_options": options}}]}}
    call_dask.ProcessorCaller.set_payload_s3_secrets(payload)
    assert json.loads(base64.b64decode(os.environ["S3_SECRETS_JSON_BASE64"])) == {"s3input": credential}
    assert os.environ["S3_KEY"] == credential["key"]
    assert os.environ["S3_SECRET"] == credential["secret"]
    assert os.environ["S3_URL"] == credential["endpoint_url"]
    assert os.environ["S3_REGION"] == credential["region_name"]


def test_payload_s3_secrets_clears_previous_job_credentials(monkeypatch):
    """A payload without input credentials must not reuse a previous job's secrets."""
    monkeypatch.setenv("S3_SECRETS_JSON_BASE64", "stale")
    call_dask.ProcessorCaller.set_payload_s3_secrets({})
    assert "S3_SECRETS_JSON_BASE64" not in os.environ


def test_payload_s3_secrets_rejects_ambiguous_inputs(monkeypatch):
    """Different input credentials cannot silently share one alias."""
    monkeypatch.setenv("S3_SECRETS_JSON_BASE64", "stale")
    products = [
        {
            "reader_params": {
                "storage_options": {
                    "key": key,
                    "secret": "secret",  # nosec B105
                    "client_kwargs": {"endpoint_url": "https://payload.test", "region_name": "gra"},
                },
            },
        }
        for key in ("first", "second")
    ]
    with pytest.raises(ValueError, match="Multiple input credential sets"):
        call_dask.ProcessorCaller.set_payload_s3_secrets({"io": {"input_products": products}})
    assert "S3_SECRETS_JSON_BASE64" not in os.environ
