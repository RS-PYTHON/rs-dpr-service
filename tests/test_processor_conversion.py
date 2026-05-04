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

"""Tests for rs_dpr_service.processors.conversion_processor."""

import asyncio

import pytest
from pygeoapi.util import JobStatus

from rs_dpr_service.dask.call_dask import ClusterInfo
from rs_dpr_service.processors.conversion_processor import ConversionProcessor
from rs_dpr_service.processors.generic_processor import GenericProcessor


def _set_conversion_env(monkeypatch):
    """Set the environment variables required by ConversionProcessor."""
    # The processor validates S3 credentials before it reaches the Dask orchestration.
    monkeypatch.setenv("DASK_GATEWAY_ADDRESS", "http://dask-gateway.test")
    monkeypatch.setenv("S3_ACCESSKEY", "access")
    monkeypatch.setenv("S3_SECRETKEY", "secret")
    monkeypatch.setenv("S3_ENDPOINT", "https://s3.test")
    monkeypatch.setenv("S3_REGION", "region")


def _build_processor(mocker):
    """Create a ConversionProcessor with mocked database access."""
    return ConversionProcessor(
        db_process_manager=mocker.Mock(),
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
    )


def _drive_execute(processor, data):
    """Drive execute() manually so mocked event loops can run deterministically."""
    # execute() is async; send(None) lets the fake loop run the nested processor coroutine inline.
    execute_coro = processor.execute(data)
    with pytest.raises(StopIteration) as exc:
        execute_coro.send(None)
    return exc.value.value


def _valid_conversion_payload():
    """Return a valid conversion execute() payload."""
    return {
        "input_safe_path": "s3://safe-bucket/product.SAFE",
        "output_zarr_dir_path": "s3://zarr-bucket/out/",
        "safe_s3_config": {"safe": "config"},
        "zarr_s3_config": {"zarr": "config"},
    }


def test_conversion_processor_initializes_generic_processor_configuration(mocker, monkeypatch):
    """Test ConversionProcessor initializes GenericProcessor and creates a job."""
    # GenericProcessor creates a cluster handler, so keep the Dask gateway env available.
    monkeypatch.setenv("DASK_GATEWAY_ADDRESS", "http://dask-gateway.test")

    db_process_manager = mocker.Mock()
    cluster_info = ClusterInfo(jupyter_token="token", cluster_label="dask-l0")  # nosec B106

    processor = ConversionProcessor(db_process_manager=db_process_manager, cluster_info=cluster_info)

    assert isinstance(processor, GenericProcessor)
    assert processor.cluster_handler.cluster_info is cluster_info
    assert processor.cluster_handler.cluster_address == "http://dask-gateway.test"
    db_process_manager.add_job.assert_called_once()


def test_execute_runs_nominal_conversion_flow_with_mocked_s3_and_dask(mocker, monkeypatch):
    """Test execute() nominal flow with S3 and Dask external boundaries mocked."""
    _set_conversion_env(monkeypatch)

    class FakeLoop:
        """Minimal event loop used to drive ConversionProcessor.execute."""

        @staticmethod
        def is_running():
            """Return False so execute() drives start_processor() synchronously."""
            return False

        @staticmethod
        def run_until_complete(start_processor_coroutine):
            """Run the coroutine to completion in the test loop."""
            return asyncio.run(start_processor_coroutine)

    async def run_inline(func, *args, **kwargs):
        # The production code sends manage_dask_tasks() to a thread; run it inline for stable assertions.
        """Run asyncio.to_thread() work inline for deterministic assertions."""
        return func(*args, **kwargs)

    db_process_manager = mocker.Mock()
    processor = ConversionProcessor(
        db_process_manager=db_process_manager,
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
    )

    # Mock S3 checks, but let _check_s3_config/_check_input_output_uris/_check_write_permission run.
    s3_fs = mocker.Mock()
    s3_fs.exists.side_effect = lambda path: path in {"safe-bucket/product.SAFE", "zarr-bucket"}
    s3_fs.open = mocker.mock_open()
    mocker.patch("rs_dpr_service.processors.conversion_processor.fsspec.filesystem", return_value=s3_fs)

    # Mock Dask execution, but let manage_dask_tasks() build cfg and consume the future.
    future = mocker.Mock()
    future.result.return_value = "s3://zarr-bucket/out/product.zarr"
    dask_client = mocker.Mock()
    dask_client.submit.return_value = future
    setup_dask_connection = mocker.patch.object(
        processor.cluster_handler,
        "setup_dask_connection",
        return_value=dask_client,
    )

    mocker.patch("rs_dpr_service.processors.generic_processor.asyncio.get_event_loop", return_value=FakeLoop())
    mocker.patch("rs_dpr_service.processors.generic_processor.asyncio.to_thread", side_effect=run_inline)

    data = {
        "input_safe_path": "s3://safe-bucket/product.SAFE",
        "output_zarr_dir_path": "s3://zarr-bucket/out/",
        "safe_s3_config": {"safe": "config"},
        "zarr_s3_config": {"zarr": "config"},
    }

    execute_coro = processor.execute(data)
    with pytest.raises(StopIteration) as exc:
        execute_coro.send(None)
    result = exc.value.value

    setup_dask_connection.assert_called_once_with()
    s3_fs.ls.assert_called_once_with("/")
    s3_fs.exists.assert_any_call("safe-bucket/product.SAFE")
    s3_fs.exists.assert_any_call("zarr-bucket")
    s3_fs.open.assert_called_once()
    s3_fs.rm.assert_called_once()
    dask_client.submit.assert_called_once()
    # The conversion processor derives the output Zarr URI from the SAFE basename.
    assert dask_client.submit.call_args.args[1] == {
        "safe_uri": "s3://safe-bucket/product.SAFE",
        "zarr_uri": "s3://zarr-bucket/out/product.zarr",
        "safe_s3_config": {"safe": "config"},
        "zarr_s3_config": {"zarr": "config"},
    }
    future.result.assert_called_once_with()
    # ConversionProcessor.manage_dask_tasks() and GenericProcessor.start_processor() both close the client.
    assert dask_client.close.call_count == 2
    assert result == ("application/json", {"successful": processor.job_logger.job_id})
    assert db_process_manager.update_job.call_args_list[-1].args[1]["status"] == JobStatus.successful.value
    assert db_process_manager.update_job.call_args_list[-1].args[1]["progress"] == 100


@pytest.mark.parametrize(
    (
        "case_name",
        "payload_updates",
        "env_to_delete",
        "filesystem_side_effect",
        "exists_result",
        "open_side_effect",
    ),
    [
        # These cases cover validation failures before any conversion job is submitted to Dask.
        ("missing_env", {}, "S3_SECRETKEY", None, True, None),
        ("s3_connection_error", {}, None, RuntimeError("S3 connection failed"), True, None),
        ("invalid_input_uri", {"input_safe_path": "file://product.SAFE"}, None, None, True, None),
        ("invalid_output_uri", {"output_zarr_dir_path": "file://zarr/out"}, None, None, True, None),
        ("missing_safe_input", {}, None, None, {"safe-bucket/product.SAFE": False, "zarr-bucket": True}, None),
        ("missing_output_bucket", {}, None, None, {"safe-bucket/product.SAFE": True, "zarr-bucket": False}, None),
        ("access_denied", {}, None, None, True, Exception("AccessDenied")),
        ("write_permission_error", {}, None, None, True, Exception("Cannot create test object")),
    ],
)
def test_execute_marks_job_failed_when_conversion_validation_fails(
    mocker,
    monkeypatch,
    case_name,
    payload_updates,
    env_to_delete,
    filesystem_side_effect,
    exists_result,
    open_side_effect,
):
    """Test execute() marks the conversion job failed when validation fails."""
    _set_conversion_env(monkeypatch)
    if env_to_delete:
        monkeypatch.delenv(env_to_delete)

    # Each parametrized case fails before the Dask orchestration starts.
    data = _valid_conversion_payload()
    data.update(payload_updates)

    processor = _build_processor(mocker)
    s3_fs = mocker.Mock()
    # Some cases need path-specific existence checks; others can use a single boolean result.
    if isinstance(exists_result, dict):
        s3_fs.exists.side_effect = lambda path: exists_result.get(path, False)
    else:
        s3_fs.exists.side_effect = lambda _path: exists_result
    s3_fs.open = mocker.Mock(side_effect=open_side_effect) if open_side_effect else mocker.mock_open()
    filesystem = mocker.patch(
        "rs_dpr_service.processors.conversion_processor.fsspec.filesystem",
        return_value=s3_fs,
        side_effect=filesystem_side_effect,
    )

    result = asyncio.run(processor.execute(data))

    if case_name != "missing_env":
        filesystem.assert_called_once()
    assert result == ("application/json", {"failed": processor.job_logger.job_id})
    assert processor.job_logger.status == JobStatus.failed


def test_execute_marks_job_failed_when_conversion_dask_client_is_missing(mocker, monkeypatch):
    """Test execute() when conversion manage_dask_tasks() receives no Dask client."""
    _set_conversion_env(monkeypatch)

    class FakeLoop:
        """Minimal event loop used to drive ConversionProcessor.execute."""

        @staticmethod
        def is_running():
            """Return False so execute() drives start_processor() synchronously."""
            return False

        @staticmethod
        def run_until_complete(start_processor_coroutine):
            """Run the coroutine to completion in the test loop."""
            return asyncio.run(start_processor_coroutine)

    async def run_inline(func, *args, **kwargs):
        # This keeps the failure inside manage_dask_tasks() visible to the current test.
        """Run asyncio.to_thread() work inline for deterministic assertions."""
        return func(*args, **kwargs)

    processor = _build_processor(mocker)
    s3_fs = mocker.Mock()
    s3_fs.exists.return_value = True
    s3_fs.open = mocker.mock_open()
    mocker.patch("rs_dpr_service.processors.conversion_processor.fsspec.filesystem", return_value=s3_fs)
    mocker.patch.object(processor.cluster_handler, "setup_dask_connection", return_value=None)
    mocker.patch("rs_dpr_service.processors.generic_processor.asyncio.get_event_loop", return_value=FakeLoop())
    mocker.patch("rs_dpr_service.processors.generic_processor.asyncio.to_thread", side_effect=run_inline)

    result = _drive_execute(processor, _valid_conversion_payload())

    assert result == ("application/json", {"failed": processor.job_logger.job_id})
    assert "Error from tasks monitoring thread:" in processor.job_logger.message


@pytest.mark.parametrize("dask_failure", ["submit", "result"])
def test_execute_marks_job_failed_when_conversion_dask_processing_fails(mocker, monkeypatch, dask_failure):
    """Test execute() when conversion Dask submission or result retrieval fails."""
    _set_conversion_env(monkeypatch)

    class FakeLoop:
        """Minimal event loop used to drive ConversionProcessor.execute."""

        @staticmethod
        def is_running():
            """Return False so execute() drives start_processor() synchronously."""
            return False

        @staticmethod
        def run_until_complete(start_processor_coroutine):
            """Run the coroutine to completion in the test loop."""
            return asyncio.run(start_processor_coroutine)

    async def run_inline(func, *args, **kwargs):
        # Avoid real threading so submit/result failures are asserted synchronously.
        """Run asyncio.to_thread() work inline for deterministic assertions."""
        return func(*args, **kwargs)

    processor = _build_processor(mocker)
    s3_fs = mocker.Mock()
    s3_fs.exists.return_value = True
    s3_fs.open = mocker.mock_open()
    mocker.patch("rs_dpr_service.processors.conversion_processor.fsspec.filesystem", return_value=s3_fs)

    future = mocker.Mock()
    future.result.side_effect = RuntimeError("Conversion task result retrieval failed")
    dask_client = mocker.Mock()
    if dask_failure == "submit":
        # Failure while submitting the conversion job to Dask.
        dask_client.submit.side_effect = RuntimeError("Conversion task submission failed")
    else:
        # Failure while waiting for the already submitted conversion job.
        dask_client.submit.return_value = future
    mocker.patch.object(processor.cluster_handler, "setup_dask_connection", return_value=dask_client)
    mocker.patch("rs_dpr_service.processors.generic_processor.asyncio.get_event_loop", return_value=FakeLoop())
    mocker.patch("rs_dpr_service.processors.generic_processor.asyncio.to_thread", side_effect=run_inline)

    result = _drive_execute(processor, _valid_conversion_payload())

    assert result == ("application/json", {"failed": processor.job_logger.job_id})
    assert processor.job_logger.status == JobStatus.failed
    assert processor.job_logger.message.startswith("Conversion failed:")
    assert dask_client.close.call_count == 2
