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

"""Tests for rs_dpr_service.processors.generic_processor."""

import asyncio
import os
import sys
import types
from contextlib import contextmanager

import pytest
from pygeoapi.util import JobStatus

from rs_dpr_service.dask.call_dask import ClusterInfo
from rs_dpr_service.processors.generic_processor import GenericProcessor


def test_execute_runs_internal_orchestration_with_dask_future(mocker, monkeypatch):
    """Test execute() on the nominal path with a mocked Dask future."""
    monkeypatch.setenv("DASK_GATEWAY_ADDRESS", "http://dask-gateway.test")

    # ProcessorCaller imports s3fs in __init__, so provide the smallest possible stub.
    fake_s3fs = types.ModuleType("s3fs")
    setattr(fake_s3fs, "S3FileSystem", type("S3FileSystem", (), {}))
    mocker.patch.dict(sys.modules, {"s3fs": fake_s3fs})

    @contextmanager
    def fake_span():
        span = mocker.Mock()
        span.get_span_context.return_value = mocker.Mock()
        yield span

    class FakeLoop:
        """Minimal event loop used to drive GenericProcessor.execute."""

        @staticmethod
        def is_running():
            """Return a stopped-loop state for this test scenario."""
            return False

        @staticmethod
        def run_until_complete(coro):
            """Execute the awaited coroutine inline for the test."""
            return asyncio.run(coro)

    db_process_manager = mocker.Mock()
    processor = GenericProcessor(
        db_process_manager=db_process_manager,
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
    )

    future = mocker.Mock()
    future.result.return_value = {"result": "ok"}
    dask_client = mocker.Mock()
    dask_client.submit.return_value = future

    # Keep the real start_processor/manage_dask_tasks flow, but execute the thread handoff inline.
    async def run_inline(func, *args, **kwargs):
        return func(*args, **kwargs)

    mocker.patch(
        "rs_dpr_service.processors.generic_processor.start_span",
        side_effect=lambda *args, **kwargs: fake_span(),
    )
    mocker.patch("rs_dpr_service.processors.generic_processor.asyncio.get_event_loop", return_value=FakeLoop())
    mocker.patch("rs_dpr_service.processors.generic_processor.asyncio.to_thread", side_effect=run_inline)
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")
    setup_dask_connection = mocker.patch.object(
        processor.cluster_handler,
        "setup_dask_connection",
        return_value=dask_client,
    )

    # execute() is async; drive the coroutine manually so we can keep control over the mocked loop.
    execute_coro = processor.execute({"input": "value"})
    with pytest.raises(StopIteration) as exc:
        execute_coro.send(None)  # pylint: disable=no-member
    result = exc.value.value

    # The normal flow should go through Dask submission, complete the future, and mark the job successful.
    setup_dask_connection.assert_called_once_with()
    dask_client.submit.assert_called_once()
    dask_client.close.assert_called_once_with()
    assert result == ("application/json", {"successful": processor.job_logger.job_id})
    assert db_process_manager.update_job.call_args_list[-1].args[1]["status"] == JobStatus.successful.value
    assert db_process_manager.update_job.call_args_list[-1].args[1]["progress"] == 100


@pytest.mark.parametrize(
    ("side_effect", "message_fragment"),
    [
        (RuntimeError("dask down"), "Failed to start the dpr-service process:"),
        (KeyError("DASK_GATEWAY_ADDRESS"), "No env var 'DASK_GATEWAY_ADDRESS' found"),
    ],
)
def test_execute_marks_job_failed_when_dask_connection_setup_fails(mocker, side_effect, message_fragment):
    """Test execute() when start_processor() fails to create the Dask client."""
    mocker.patch.dict(sys.modules, {"s3fs": types.ModuleType("s3fs")})

    os.environ["DASK_GATEWAY_ADDRESS"] = "http://dask-gateway.test"

    class FakeLoop:
        """Minimal event loop used to drive GenericProcessor.execute."""

        @staticmethod
        def is_running():
            """Return a stopped-loop state for this test scenario."""
            return False

        @staticmethod
        def run_until_complete(coro):
            """Execute the awaited coroutine inline for the test."""
            return asyncio.run(coro)

    db_process_manager = mocker.Mock()
    processor = GenericProcessor(
        db_process_manager=db_process_manager,
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
    )

    mocker.patch("rs_dpr_service.processors.generic_processor.asyncio.get_event_loop", return_value=FakeLoop())
    mocker.patch.object(processor.cluster_handler, "setup_dask_connection", side_effect=side_effect)

    execute_coro = processor.execute({"input": "value"})
    with pytest.raises(StopIteration) as exc:
        execute_coro.send(None)  # pylint: disable=no-member
    result = exc.value.value

    assert result == ("application/json", {"failed": processor.job_logger.job_id})
    assert db_process_manager.update_job.call_args_list[-1].args[1]["status"] == JobStatus.failed.value
    assert db_process_manager.update_job.call_args_list[-1].args[1]["progress"] == 0
    assert message_fragment in db_process_manager.update_job.call_args_list[-1].args[1]["message"]


def test_execute_replaces_mockup_placeholders_before_submitting_to_dask(mocker, monkeypatch):
    """Test execute() when mockup placeholders are replaced before Dask submission."""
    monkeypatch.setenv("DASK_GATEWAY_ADDRESS", "http://dask-gateway.test")
    monkeypatch.setenv("FOUND_VALUE", "resolved")

    fake_s3fs = types.ModuleType("s3fs")
    setattr(fake_s3fs, "S3FileSystem", type("S3FileSystem", (), {}))
    mocker.patch.dict(sys.modules, {"s3fs": fake_s3fs})

    @contextmanager
    def fake_span():
        span = mocker.Mock()
        span.get_span_context.return_value = mocker.Mock()
        yield span

    class FakeLoop:
        """Minimal event loop used to drive GenericProcessor.execute."""

        @staticmethod
        def is_running():
            """Return a stopped-loop state for this test scenario."""
            return False

        @staticmethod
        def run_until_complete(coro):
            """Execute the awaited coroutine inline for the test."""
            return asyncio.run(coro)

    async def run_inline(func, *args, **kwargs):
        return func(*args, **kwargs)

    db_process_manager = mocker.Mock()
    processor = GenericProcessor(
        db_process_manager=db_process_manager,
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
    )
    processor.use_mockup = True

    dask_client = mocker.Mock()
    dask_client.submit.return_value.result.return_value = {"result": "ok"}

    warning = mocker.patch("rs_dpr_service.processors.generic_processor.logger.warning")
    mocker.patch(
        "rs_dpr_service.processors.generic_processor.start_span",
        side_effect=lambda *args, **kwargs: fake_span(),
    )
    mocker.patch("rs_dpr_service.processors.generic_processor.asyncio.get_event_loop", return_value=FakeLoop())
    mocker.patch("rs_dpr_service.processors.generic_processor.asyncio.to_thread", side_effect=run_inline)
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")
    mocker.patch.object(processor.cluster_handler, "setup_dask_connection", return_value=dask_client)

    execute_coro = processor.execute(
        {
            "item": "${FOUND_VALUE}",
            "nested": ["${FOUND_VALUE}", "${MISSING_VALUE}", 7],
            "deep": {"path": "prefix-${FOUND_VALUE}"},
        },
    )
    with pytest.raises(StopIteration) as exc:
        execute_coro.send(None)  # pylint: disable=no-member
    result = exc.value.value

    submitted_processor = dask_client.submit.call_args.args[0].__self__
    assert submitted_processor.data == {
        "item": "resolved",
        "nested": ["resolved", "${MISSING_VALUE}", 7],
        "deep": {"path": "prefix-resolved"},
    }
    warning.assert_called_once_with(
        "Environment variable '%s' not found; leaving placeholder unchanged.",
        "MISSING_VALUE",
    )
    assert result == ("application/json", {"successful": processor.job_logger.job_id})


def test_execute_schedules_start_processor_when_event_loop_is_already_running(mocker, monkeypatch):
    """Test execute() when it schedules start_processor() on an already running loop."""
    monkeypatch.setenv("DASK_GATEWAY_ADDRESS", "http://dask-gateway.test")

    class FakeLoop:
        """Minimal event loop used to drive GenericProcessor.execute."""

        @staticmethod
        def is_running():
            """Return a running-loop state for this scheduling test."""
            return True

    scheduled = []

    def fake_create_task(coro):
        scheduled.append(coro)
        # In this branch execute() should only schedule start_processor().
        # We intentionally do not run the coroutine here because this test targets
        # the scheduling path itself, not the full orchestration that follows.
        # Close it explicitly to avoid an un-awaited coroutine warning in the test.
        coro.close()
        return mocker.Mock()

    db_process_manager = mocker.Mock()
    processor = GenericProcessor(
        db_process_manager=db_process_manager,
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
    )

    mocker.patch("rs_dpr_service.processors.generic_processor.asyncio.get_event_loop", return_value=FakeLoop())
    create_task = mocker.patch(
        "rs_dpr_service.processors.generic_processor.asyncio.create_task",
        side_effect=fake_create_task,
    )
    # If execute() takes the scheduling branch correctly, it should not reach the Dask setup at all.
    setup_dask_connection = mocker.patch.object(processor.cluster_handler, "setup_dask_connection")

    execute_coro = processor.execute({"input": "value"})
    with pytest.raises(StopIteration) as exc:
        execute_coro.send(None)  # pylint: disable=no-member
    result = exc.value.value

    create_task.assert_called_once()
    assert len(scheduled) == 1
    setup_dask_connection.assert_not_called()
    assert result == ("application/json", {"running": processor.job_logger.job_id})
    assert db_process_manager.update_job.call_args_list[-1].args[1]["status"] == JobStatus.running.value


def test_execute_runs_processor_inline_when_local_cluster_is_enabled_in_local_mode(mocker, monkeypatch):
    """Test execute() when local mode bypasses Dask client creation."""
    monkeypatch.setenv("DASK_GATEWAY_ADDRESS", "http://dask-gateway.test")

    # ProcessorCaller imports s3fs in __init__, so keep the same lightweight stub.
    fake_s3fs = types.ModuleType("s3fs")
    setattr(fake_s3fs, "S3FileSystem", type("S3FileSystem", (), {}))
    mocker.patch.dict(sys.modules, {"s3fs": fake_s3fs})

    @contextmanager
    def fake_span():
        span = mocker.Mock()
        span.get_span_context.return_value = mocker.Mock()
        yield span

    class FakeLoop:
        """Minimal event loop used to drive GenericProcessor.execute."""

        @staticmethod
        def is_running():
            """Return a stopped-loop state for this test scenario."""
            return False

        @staticmethod
        def run_until_complete(coro):
            """Execute the awaited coroutine inline for the test."""
            return asyncio.run(coro)

    # Keep the async/thread boundary inline so we execute the local branch end-to-end.
    async def run_inline(func, *args, **kwargs):
        return func(*args, **kwargs)

    db_process_manager = mocker.Mock()
    processor = GenericProcessor(
        db_process_manager=db_process_manager,
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
    )

    # Branch under test: LOCAL_MODE + local_cluster.enabled => dask_client = None.
    mocker.patch("rs_dpr_service.processors.generic_processor.LOCAL_MODE", True)
    setup_dask_connection = mocker.patch.object(processor.cluster_handler, "setup_dask_connection")

    # Mock the worker boundary so manage_dask_tasks() runs locally without real processor code.
    run_processor = mocker.patch(
        "rs_dpr_service.dask.call_dask.ProcessorCaller.run_processor",
        return_value={"result": "local"},
    )

    mocker.patch(
        "rs_dpr_service.processors.generic_processor.start_span",
        side_effect=lambda *args, **kwargs: fake_span(),
    )
    mocker.patch("rs_dpr_service.processors.generic_processor.asyncio.get_event_loop", return_value=FakeLoop())
    mocker.patch("rs_dpr_service.processors.generic_processor.asyncio.to_thread", side_effect=run_inline)
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")

    payload = {"experimental_config": {"local_cluster": {"enabled": True}}}
    execute_coro = processor.execute(payload)
    with pytest.raises(StopIteration) as exc:
        execute_coro.send(None)  # pylint: disable=no-member
    result = exc.value.value

    setup_dask_connection.assert_not_called()
    run_processor.assert_called_once_with()
    assert result == ("application/json", {"successful": processor.job_logger.job_id})
    assert db_process_manager.update_job.call_args_list[-1].args[1]["status"] == JobStatus.successful.value
    assert db_process_manager.update_job.call_args_list[-1].args[1]["progress"] == 100


def test_execute_marks_job_failed_when_tasks_monitoring_thread_raises(mocker):
    """Test execute() when the tasks monitoring thread handoff raises an error."""

    class FakeLoop:
        """Minimal event loop used to drive GenericProcessor.execute."""

        @staticmethod
        def is_running():
            """Return a stopped-loop state for this test scenario."""
            return False

        @staticmethod
        def run_until_complete(coro):
            """Execute the awaited coroutine inline for the test."""
            return asyncio.run(coro)

    db_process_manager = mocker.Mock()
    processor = GenericProcessor(
        db_process_manager=db_process_manager,
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
    )

    dask_client = mocker.Mock()

    # Keep Dask setup successful so the failure comes only from asyncio.to_thread().
    setup_dask_connection = mocker.patch.object(
        processor.cluster_handler,
        "setup_dask_connection",
        return_value=dask_client,
    )

    # Branch under test: the monitoring thread handoff raises and start_processor() marks the job failed.
    mocker.patch("rs_dpr_service.processors.generic_processor.asyncio.get_event_loop", return_value=FakeLoop())
    mocker.patch(
        "rs_dpr_service.processors.generic_processor.asyncio.to_thread",
        side_effect=RuntimeError("thread boom"),
    )

    execute_coro = processor.execute({"input": "value"})
    with pytest.raises(StopIteration) as exc:
        execute_coro.send(None)  # pylint: disable=no-member
    result = exc.value.value

    setup_dask_connection.assert_called_once_with()
    dask_client.close.assert_called_once_with()
    assert result == ("application/json", {"failed": processor.job_logger.job_id})
    assert db_process_manager.update_job.call_args_list[-1].args[1]["status"] == JobStatus.failed.value
    assert db_process_manager.update_job.call_args_list[-1].args[1]["progress"] == 0
    assert "Error from tasks monitoring thread:" in db_process_manager.update_job.call_args_list[-1].args[1]["message"]


def test_manage_dask_tasks_marks_job_failed_when_submit_to_dask_raises(mocker):
    """Test manage_dask_tasks() when task submission to Dask fails."""
    fake_s3fs = types.ModuleType("s3fs")
    setattr(fake_s3fs, "S3FileSystem", type("S3FileSystem", (), {}))
    mocker.patch.dict(sys.modules, {"s3fs": fake_s3fs})

    @contextmanager
    def fake_span():
        span = mocker.Mock()
        span.get_span_context.return_value = mocker.Mock()
        yield span

    db_process_manager = mocker.Mock()
    processor = GenericProcessor(
        db_process_manager=db_process_manager,
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
    )

    dask_client = mocker.Mock()

    # Branch under test: dask_client exists, but submit() raises inside the first try block.
    dask_client.submit.side_effect = RuntimeError("submit boom")

    # Mock tracing/ip helpers so we stay focused on the submit failure branch.
    record_error = mocker.patch("rs_dpr_service.processors.generic_processor.record_error")
    mocker.patch(
        "rs_dpr_service.processors.generic_processor.start_span",
        side_effect=lambda *args, **kwargs: fake_span(),
    )
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")

    processor.manage_dask_tasks(dask_client, {"input": "value"})

    dask_client.submit.assert_called_once()
    record_error.assert_called_once()
    assert db_process_manager.update_job.call_args_list[-1].args[1]["status"] == JobStatus.failed.value
    assert (
        "Submitting task to dask cluster failed:" in db_process_manager.update_job.call_args_list[-1].args[1]["message"]
    )


def test_manage_dask_tasks_marks_job_failed_when_dask_task_result_raises(mocker):
    """Test manage_dask_tasks() when the submitted Dask task result raises."""
    fake_s3fs = types.ModuleType("s3fs")
    setattr(fake_s3fs, "S3FileSystem", type("S3FileSystem", (), {}))
    mocker.patch.dict(sys.modules, {"s3fs": fake_s3fs})

    @contextmanager
    def fake_span():
        span = mocker.Mock()
        span.get_span_context.return_value = mocker.Mock()
        yield span

    db_process_manager = mocker.Mock()
    processor = GenericProcessor(
        db_process_manager=db_process_manager,
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
    )

    dask_client = mocker.Mock()
    dpr_task = mocker.Mock()
    dpr_task.result.side_effect = RuntimeError("result boom")
    dpr_task.key = "task-key"

    # Branch under test: submit() succeeds, but dpr_task.result() raises in the second try block.
    dask_client.submit.return_value = dpr_task

    # Mock tracing/ip helpers so we stay focused on the task result failure branch.
    record_error = mocker.patch("rs_dpr_service.processors.generic_processor.record_error")
    mocker.patch(
        "rs_dpr_service.processors.generic_processor.start_span",
        side_effect=lambda *args, **kwargs: fake_span(),
    )
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")

    processor.manage_dask_tasks(dask_client, {"input": "value"})

    dask_client.submit.assert_called_once()
    dpr_task.result.assert_called_once_with()
    record_error.assert_called_once()
    assert db_process_manager.update_job.call_args_list[-1].args[1]["status"] == JobStatus.failed.value
    assert "Processing task failed:" in db_process_manager.update_job.call_args_list[-1].args[1]["message"]


def test_manage_dask_tasks_reraises_when_local_run_processor_raises_without_dask_client(mocker):
    """Test manage_dask_tasks() when local execution raises without a Dask client."""
    fake_s3fs = types.ModuleType("s3fs")
    setattr(fake_s3fs, "S3FileSystem", type("S3FileSystem", (), {}))
    mocker.patch.dict(sys.modules, {"s3fs": fake_s3fs})

    @contextmanager
    def fake_span():
        span = mocker.Mock()
        span.get_span_context.return_value = mocker.Mock()
        yield span

    db_process_manager = mocker.Mock()
    processor = GenericProcessor(
        db_process_manager=db_process_manager,
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
    )

    # Branch under test: dask_client is None and local run_processor() raises in the first try block.
    run_processor = mocker.patch(
        "rs_dpr_service.dask.call_dask.ProcessorCaller.run_processor",
        side_effect=RuntimeError("local boom"),
    )

    # Mock span/ip helpers and assert the local error is logged then re-raised.
    logger_exception = mocker.patch("rs_dpr_service.processors.generic_processor.logger.exception")
    mocker.patch(
        "rs_dpr_service.processors.generic_processor.start_span",
        side_effect=lambda *args, **kwargs: fake_span(),
    )
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")

    with pytest.raises(RuntimeError, match="local boom"):
        processor.manage_dask_tasks(None, {"input": "value"})

    run_processor.assert_called_once_with()
    logger_exception.assert_called_once()


def test_get_tasktable_returns_dask_result_and_closes_client(mocker):
    """Test get_tasktable() on the nominal path and close the Dask client."""
    fake_s3fs = types.ModuleType("s3fs")
    setattr(fake_s3fs, "S3FileSystem", type("S3FileSystem", (), {}))
    mocker.patch.dict(sys.modules, {"s3fs": fake_s3fs})

    db_process_manager = mocker.Mock()
    processor = GenericProcessor(
        db_process_manager=db_process_manager,
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
        tasktable_module="fake.module",
        tasktable_class="FakeTaskTable",
    )

    current_span = mocker.Mock()
    current_span.get_span_context.return_value = mocker.Mock()
    future = mocker.Mock()
    future.result.return_value = {"tasktable": "value"}
    dask_client = mocker.Mock()
    dask_client.submit.return_value = future

    # Branch under test: setup succeeds, submit succeeds, result() is returned unchanged.
    mocker.patch("rs_dpr_service.processors.generic_processor.trace.get_current_span", return_value=current_span)
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")
    setup_dask_connection = mocker.patch.object(
        processor.cluster_handler,
        "setup_dask_connection",
        return_value=dask_client,
    )

    result = asyncio.run(processor.get_tasktable())

    setup_dask_connection.assert_called_once_with()
    dask_client.submit.assert_called_once()
    dask_client.close.assert_called_once_with()
    assert result == {"tasktable": "value"}


def test_get_tasktable_returns_mockup_fallback_when_dask_result_is_empty(mocker):
    """Test get_tasktable() when mockup mode loads the fallback tasktable file."""
    fake_s3fs = types.ModuleType("s3fs")
    setattr(fake_s3fs, "S3FileSystem", type("S3FileSystem", (), {}))
    mocker.patch.dict(sys.modules, {"s3fs": fake_s3fs})

    class FakeAsyncFile:
        """Minimal async file wrapper for the mockup fallback path."""

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def read(self):
            """Return the mocked tasktable payload."""
            return '{"mockup": true}'

    async def fake_open_file(*_args, **_kwargs):
        return FakeAsyncFile()

    db_process_manager = mocker.Mock()
    processor = GenericProcessor(
        db_process_manager=db_process_manager,
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
        tasktable_module="fake.module",
        tasktable_class="FakeTaskTable",
    )
    processor.use_mockup = True

    current_span = mocker.Mock()
    current_span.get_span_context.return_value = mocker.Mock()
    future = mocker.Mock()
    future.result.return_value = {}
    dask_client = mocker.Mock()
    dask_client.submit.return_value = future

    # Branch under test: empty Dask result + use_mockup=True => fallback file is loaded.
    mocker.patch("rs_dpr_service.processors.generic_processor.trace.get_current_span", return_value=current_span)
    mocker.patch("rs_dpr_service.processors.generic_processor.anyio.open_file", side_effect=fake_open_file)
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")
    setup_dask_connection = mocker.patch.object(
        processor.cluster_handler,
        "setup_dask_connection",
        return_value=dask_client,
    )

    result = asyncio.run(processor.get_tasktable())

    setup_dask_connection.assert_called_once_with()
    dask_client.submit.assert_called_once()
    dask_client.close.assert_called_once_with()
    assert result == {"mockup": True}


def test_get_tasktable_reraises_when_dask_tasktable_retrieval_fails(mocker):
    """Test get_tasktable() when the Dask tasktable retrieval raises an exception."""
    fake_s3fs = types.ModuleType("s3fs")
    setattr(fake_s3fs, "S3FileSystem", type("S3FileSystem", (), {}))
    mocker.patch.dict(sys.modules, {"s3fs": fake_s3fs})

    db_process_manager = mocker.Mock()
    processor = GenericProcessor(
        db_process_manager=db_process_manager,
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
        tasktable_module="fake.module",
        tasktable_class="FakeTaskTable",
    )

    current_span = mocker.Mock()
    current_span.get_span_context.return_value = mocker.Mock()
    future = mocker.Mock()
    future.result.side_effect = RuntimeError("tasktable boom")
    dask_client = mocker.Mock()
    dask_client.submit.return_value = future

    # Branch under test: result() raises, logger.exception() runs, and finally closes the client.
    logger_exception = mocker.patch("rs_dpr_service.processors.generic_processor.logger.exception")
    mocker.patch("rs_dpr_service.processors.generic_processor.trace.get_current_span", return_value=current_span)
    mocker.patch("rs_dpr_service.dask.call_dask.get_ip_address", return_value="127.0.0.1")
    setup_dask_connection = mocker.patch.object(
        processor.cluster_handler,
        "setup_dask_connection",
        return_value=dask_client,
    )

    with pytest.raises(RuntimeError, match="tasktable boom"):
        asyncio.run(processor.get_tasktable())

    setup_dask_connection.assert_called_once_with()
    dask_client.submit.assert_called_once()
    logger_exception.assert_called_once()
    dask_client.close.assert_called_once_with()
