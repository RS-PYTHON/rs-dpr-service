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
Implement tests that are common to several services.

NOTE: COPY-PASTED FROM pytest_common_tests.py in RS-SERVER.
"""

import contextlib
import copy
import json
from datetime import datetime
from importlib import reload
from unittest.mock import AsyncMock

import pytest
from starlette.exceptions import HTTPException as StarletteHTTPException

import rs_dpr_service.main as main_module
from rs_dpr_service.main import (
    ClusterInfo,
    DatabaseJobFormatError,
    JobsFormatError,
    build_cluster_info,
    format_job_data,
    format_jobs_data,
    init_db,
)


def job_record(identifier: str, status: str = "running") -> dict:
    """Create a database-shaped job payload for endpoint tests."""
    return {
        "identifier": identifier,
        "status": status,
        "type": "process",
        "progress": 55 if status == "running" else 100,
        "message": "Test detail",
        "created": datetime(2026, 4, 20, 10, 0, 0),
        "updated": datetime(2026, 4, 20, 11, 0, 0),
        "processID": "mockup",
    }


def test_build_cluster_info_all_fields():
    """Test the default behaviour for all parameters set."""
    data = {"jupyter_token": "jupyter", "cluster_label": "dask-l0", "cluster_instance": "instance-1"}  # nosec B105

    result = build_cluster_info(data)

    assert isinstance(result, ClusterInfo)
    assert result.jupyter_token == "jupyter"  # nosec B105
    assert result.cluster_label == "dask-l0"  # nosec B105
    assert result.cluster_instance == "instance-1"  # nosec B105


def test_build_cluster_info_without_cluster_instance():
    """Test if the optional parameter is set to default value."""
    data = {
        "jupyter_token": "jupyter",  # nosec B105
        "cluster_label": "dask-l0",  # nosec B105
    }

    result = build_cluster_info(data)

    assert result.jupyter_token == "jupyter"  # nosec B105
    assert result.cluster_label == "dask-l0"  # nosec B105
    assert result.cluster_instance == ""


def test_build_cluster_info_missing_jupyter_token():
    """Test if an error is raised when required parameters are not supplied."""
    data = {
        "cluster_label": "dask-l0",
    }

    with pytest.raises(StarletteHTTPException) as exc:
        build_cluster_info(data)

    assert exc.value.status_code == 400
    assert "Missing required fields" in exc.value.detail


def test_format_jobs_data_missing_jobs_key():
    """Test that format_jobs_data raises when the 'jobs' key is missing."""
    with pytest.raises(JobsFormatError) as exc:
        format_jobs_data({"attr1": "val1", "attr2": "val2"})

    assert "missing 'jobs' key" in str(exc.value)


def test_format_jobs_data_wrong_input_type():
    """Test that format_jobs_data raises when input is not a dictionary."""
    with pytest.raises(JobsFormatError) as exc:
        format_jobs_data("wrong_data")  # type: ignore

    assert "Expected a dictionary as input" in str(exc.value)


def test_format_jobs_data_valid_input():
    """Test that format_jobs_data adds links and formats each job entry."""
    created = datetime(2026, 4, 19, 10, 30, 0)
    updated = datetime(2026, 4, 19, 11, 45, 0)
    jobs = {
        "jobs": [
            {
                "identifier": "job-1",
                "status": "running",
                "created": created,
                "updated": updated,
                "finished": None,
                "_sa_instance_state": "state",
                "location": "result-path",
                "mimetype": "application/json",
            },
        ],
    }

    result = format_jobs_data(jobs)

    assert "links" in result
    assert result["links"][0]["title"] == "List of jobs"
    assert result["jobs"] == [
        {
            "jobID": "job-1",
            "status": "running",
            "created": "2026-04-19T10:30:00Z",
            "updated": "2026-04-19T11:45:00Z",
        },
    ]


def test_format_job_data_missing_identifier():
    """Test that format_job_data raises when identifier is missing."""
    with pytest.raises(DatabaseJobFormatError) as exc:
        format_job_data({"status": "running"})

    assert "attribute 'identifier' is missing" in str(exc.value)


def test_format_job_data_valid_input():
    """Test that format_job_data reformats a valid database job."""
    created = datetime(2026, 4, 19, 10, 30, 0)
    started = datetime(2026, 4, 19, 10, 31, 0)
    job = {
        "identifier": "job-1",
        "status": "successful",
        "created": created,
        "started": started,
        "finished": None,
        "_sa_instance_state": "state",
        "location": "result-path",
        "mimetype": "application/json",
        "message": "done",
    }

    result = format_job_data(job)

    assert result == {
        "jobID": "job-1",
        "status": "successful",
        "created": "2026-04-19T10:30:00Z",
        "started": "2026-04-19T10:31:00Z",
        "message": "done",
    }


def test_init_db_invalid_manager_definition(mocker):
    """Test that init_db raises when pygeoapi manager config is invalid."""
    mocker.patch.object(main_module, "api", mocker.Mock(config={"manager": None}))

    with pytest.raises(RuntimeError, match="Error reading the manager definition"):
        init_db()


def test_init_db_retries_then_succeeds(mocker):
    """Test that init_db retries after a database error and then returns a manager."""
    manager_def = {"connection": {"host": "db-host"}}
    engine = mocker.Mock()
    engine.url = "postgresql://db-host"
    postgres_manager = mocker.Mock(name="postgres_manager")
    create_all = mocker.patch.object(
        main_module.Base.metadata,
        "create_all",
        side_effect=[main_module.SQLAlchemyError("db down"), None],
    )
    sleep_mock = mocker.patch("rs_dpr_service.main.sleep")
    get_engine_mock = mocker.patch("rs_dpr_service.main.get_engine", return_value=engine)
    postgres_manager_mock = mocker.patch("rs_dpr_service.main.PostgreSQLManager", return_value=postgres_manager)
    mocker.patch.object(main_module, "api", mocker.Mock(config={"manager": manager_def}))

    result = init_db(pause=5, timeout=10)

    assert result is postgres_manager
    get_engine_mock.assert_called_once_with(driver_name="postgresql+psycopg2", **manager_def["connection"])
    assert create_all.call_count == 2
    sleep_mock.assert_called_once_with(5)
    postgres_manager_mock.assert_called_once_with(manager_def)


def test_init_db_raises_on_timeout(mocker):
    """Test that init_db re-raises the database error when timeout is reached."""
    manager_def = {"connection": {"host": "db-host"}}
    engine = mocker.Mock()
    engine.url = "postgresql://db-host"
    mocker.patch.object(
        main_module.Base.metadata,
        "create_all",
        side_effect=main_module.SQLAlchemyError("db down"),
    )
    sleep_mock = mocker.patch("rs_dpr_service.main.sleep")
    mocker.patch("rs_dpr_service.main.get_engine", return_value=engine)
    mocker.patch.object(main_module, "api", mocker.Mock(config={"manager": manager_def}))

    with pytest.raises(main_module.SQLAlchemyError, match="db down"):
        init_db(pause=5, timeout=0)

    sleep_mock.assert_not_called()


def test_get_processes_endpoint(client):
    """Test the endpoint that lists all available DPR processes."""
    response = client.get("/dpr/processes")

    assert response.status_code == 200
    payload = response.json()
    assert "processes" in payload
    assert "links" in payload
    assert payload["links"][0]["rel"] == "self"
    assert payload["links"][0]["title"] == "List of processes"
    assert {process["id"] for process in payload["processes"]} == set(main_module.processor_types)


def test_get_resource_endpoint_returns_404_for_unknown_resource(client):
    """Test the endpoint response when the requested process does not exist."""
    response = client.get("/dpr/processes/unknown-process")

    assert response.status_code == 404
    assert response.json()["detail"] == "Process 'unknown-process' not found"


def test_get_resource_endpoint_returns_tasktable_for_mockup_process(client, mocker, monkeypatch):
    """Test the endpoint response for a known process resource."""
    client.app.extra["process_manager"] = mocker.Mock()
    monkeypatch.setenv("DASK_GATEWAY_ADDRESS", "http://dask-gateway.test")

    response = client.get(
        "/dpr/processes/mockup",
        params={"jupyter_token": "token", "cluster_label": "dask-l0"},  # nosec B105
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["tasktable"]["name"] == "mockup-processor"
    assert payload["tasktable"]["version"] == "1.0"


def test_execute_process_returns_404_for_unknown_resource(client, mocker):
    """Test the execution endpoint when the requested process does not exist."""
    span = mocker.Mock()
    start_span_mock = mocker.patch("rs_dpr_service.main.start_span")
    start_span_mock.return_value.__enter__.return_value = span
    start_span_mock.return_value.__exit__.return_value = False
    mocker.patch(
        "rs_dpr_service.main.validate_request",
        return_value={"jupyter_token": "jupyter", "cluster_label": "dask-l0"},  # nosec B105
    )

    response = client.post("/dpr/processes/unknown-process/execution", json={})

    assert response.status_code == 404
    assert response.json()["detail"] == "Process 'unknown-process' not found"
    span.set_status.assert_called_once()


def test_execute_process_endpoint_success(client, mocker):
    """Test the execution endpoint for a valid process with a successful execution."""
    span = mocker.Mock()
    start_span_mock = mocker.patch("rs_dpr_service.main.start_span")
    start_span_mock.return_value.__enter__.return_value = span
    start_span_mock.return_value.__exit__.return_value = False
    valid_body = {"jupyter_token": "jupyter", "cluster_label": "dask-l0"}  # nosec B105

    # Let client.post() call the real validate_request(), but provide a stable request body.
    mocker.patch(
        "starlette.requests.Request.body",
        new=AsyncMock(return_value=json.dumps(valid_body).encode()),
    )
    # Mock the OpenAPI adapter/validator so only the external validation layer is bypassed.
    openapi_request_mock = mocker.patch("rs_dpr_service.openapi_validation.StarletteOpenAPIRequest")
    validate_openapi_mock = mocker.patch("rs_dpr_service.openapi_validation.OPENAPI.validate_request")
    mocker.patch("rs_dpr_service.main.validate_response")
    process_manager = mocker.Mock()
    process_manager.get_job.return_value = job_record("job-1", status="accepted")
    client.app.extra["process_manager"] = process_manager

    class SuccessfulProcessor:
        """Fake processor used to test the success path."""

        def __init__(self, db_process_manager, cluster_info):
            self.db_process_manager = db_process_manager
            self.cluster_info = cluster_info

        async def execute(self, data):
            """Return a successful DPR execution payload."""
            assert self.db_process_manager is process_manager
            assert self.cluster_info.cluster_label == "dask-l0"
            assert data == valid_body
            return "application/json", {"accepted": "job-1"}

    api_mock = mocker.Mock()
    api_mock.config = {"resources": {"mockup": {"processor": {"name": "mockup"}}}}
    mocker.patch.object(main_module, "api", api_mock)
    mocker.patch.dict(main_module.processor_types, {"mockup": SuccessfulProcessor}, clear=False)

    response = client.post("/dpr/processes/mockup/execution", json={})

    assert response.status_code == 201
    assert response.json() == format_job_data(process_manager.get_job.return_value)
    openapi_request_mock.assert_called_once()
    validate_openapi_mock.assert_called_once_with(openapi_request_mock.return_value)
    process_manager.get_job.assert_called_once_with("job-1")
    span.set_status.assert_called_once()


def test_execute_process_returns_404_for_unknown_processor_name(client, mocker):
    """Test the execution endpoint when a configured processor is missing from processor_types."""
    span = mocker.Mock()
    start_span_mock = mocker.patch("rs_dpr_service.main.start_span")
    start_span_mock.return_value.__enter__.return_value = span
    start_span_mock.return_value.__exit__.return_value = False
    mocker.patch(
        "rs_dpr_service.main.validate_request",
        return_value={"jupyter_token": "jupyter", "cluster_label": "dask-l0"},  # nosec B105
    )
    api_mock = mocker.Mock()
    api_mock.config = {"resources": {"mockup": {"processor": {"name": "unknown_processor"}}}}
    mocker.patch.object(main_module, "api", api_mock)

    response = client.post("/dpr/processes/mockup/execution", json={})

    assert response.status_code == 404
    assert response.json()["detail"] == "Processor 'unknown_processor' not found"
    span.set_status.assert_called_once()


def test_execute_process_records_error_when_processor_execution_fails(client, mocker):
    """Test the execution endpoint when the processor raises an exception."""
    span = mocker.Mock()
    start_span_mock = mocker.patch("rs_dpr_service.main.start_span")
    start_span_mock.return_value.__enter__.return_value = span
    start_span_mock.return_value.__exit__.return_value = False
    mocker.patch(
        "rs_dpr_service.main.validate_request",
        return_value={"jupyter_token": "jupyter", "cluster_label": "dask-l0"},  # nosec B105
    )
    record_error = mocker.patch("rs_dpr_service.main.record_error")
    client.app.extra["process_manager"] = mocker.Mock()

    class FailingProcessor:
        """Fake processor used to test the error path."""

        def __init__(self, db_process_manager, cluster_info):
            self.db_process_manager = db_process_manager
            self.cluster_info = cluster_info

        async def execute(self, data):
            """Raise an exception to exercise the error path."""
            raise ValueError("processor exploded")

    api_mock = mocker.Mock()
    api_mock.config = {"resources": {"mockup": {"processor": {"name": "mockup"}}}}
    mocker.patch.object(main_module, "api", api_mock)
    mocker.patch.dict(main_module.processor_types, {"mockup": FailingProcessor}, clear=False)

    response = client.post("/dpr/processes/mockup/execution", json={})

    assert response.status_code == 500
    assert response.json()["detail"] == "processor exploded"
    record_error.assert_called_once()


def test_get_job_status_endpoint_success(client, mocker):
    """Test the job status endpoint when the job exists."""
    process_manager = mocker.Mock()
    client.app.extra["process_manager"] = process_manager
    process_manager.get_job.return_value = job_record("job-1")

    response = client.get("/dpr/jobs/job-1")

    assert response.status_code == 200
    assert response.json() == format_job_data(process_manager.get_job.return_value)


def test_get_job_status_endpoint_returns_404_for_unknown_job(client, mocker):
    """Test the job status endpoint when the job does not exist."""
    process_manager = mocker.Mock()
    client.app.extra["process_manager"] = process_manager
    process_manager.get_job.side_effect = main_module.JobNotFoundError

    response = client.get("/dpr/jobs/unknown-job")

    assert response.status_code == 404
    assert response.json()["detail"] == "Job with ID unknown-job not found"


@pytest.mark.asyncio
async def test_get_job_logs_endpoint_returns_404_for_unknown_job(mocker):
    """Test the job logs endpoint when the job does not exist."""
    process_manager = mocker.Mock()
    main_module.app.extra["process_manager"] = process_manager
    process_manager.get_job.side_effect = main_module.JobNotFoundError

    response = await main_module.get_job_logs_endpoint(mocker.Mock(), "unknown-job")

    assert response.status_code == 404
    assert json.loads(response.body) == "Job with ID unknown-job not found"


@pytest.mark.asyncio
async def test_get_job_logs_endpoint_streams_queued_log(mocker):
    """Test that job logs are streamed as SSE data and the queue is cleaned up."""
    process_manager = mocker.Mock()
    main_module.app.extra["process_manager"] = process_manager
    process_manager.get_job.return_value = job_record("job-1")
    request = mocker.Mock()
    request.is_disconnected = AsyncMock(side_effect=[False, True])
    main_module.job_log_handler.queues.clear()

    response = await main_module.get_job_logs_endpoint(request, "job-1")
    queue = main_module.job_log_handler.queues["job-1"][0]
    queue.put_nowait("Processing started")
    body_iterator = response.body_iterator

    try:
        assert response.status_code == 200
        assert response.media_type == "text/event-stream"
        assert await anext(body_iterator) == "data: Processing started\n\n"
        with pytest.raises(StopAsyncIteration):
            await anext(body_iterator)
        assert "job-1" not in main_module.job_log_handler.queues
    finally:
        with contextlib.suppress(Exception):
            await body_iterator.aclose()
        main_module.job_log_handler.queues.pop("job-1", None)


@pytest.mark.asyncio
async def test_get_job_logs_endpoint_sends_keepalive_until_job_finishes(mocker):
    """Test that the log stream emits keepalive messages and stops for terminal jobs."""

    async def timeout_wait_for(awaitable, timeout):  # pylint: disable=unused-argument
        awaitable.close()
        raise TimeoutError

    process_manager = mocker.Mock()
    main_module.app.extra["process_manager"] = process_manager
    process_manager.get_job.side_effect = [
        job_record("job-1", status="running"),
        job_record("job-1", status="successful"),
    ]
    request = mocker.Mock()
    request.is_disconnected = AsyncMock(return_value=False)
    mocker.patch("rs_dpr_service.main.asyncio.wait_for", side_effect=timeout_wait_for)
    main_module.job_log_handler.queues.clear()

    response = await main_module.get_job_logs_endpoint(request, "job-1")
    body_iterator = response.body_iterator

    try:
        assert await anext(body_iterator) == ": keepalive\n\n"
        with pytest.raises(StopAsyncIteration):
            await anext(body_iterator)
        assert "job-1" not in main_module.job_log_handler.queues
    finally:
        with contextlib.suppress(Exception):
            await body_iterator.aclose()
        main_module.job_log_handler.queues.pop("job-1", None)


def test_get_jobs_endpoint_success(client, mocker):
    """Test the jobs endpoint when jobs are available."""
    process_manager = mocker.Mock()
    client.app.extra["process_manager"] = process_manager
    process_manager.get_jobs.return_value = {"jobs": [job_record("job-1")], "numberMatched": 1}

    response = client.get("/dpr/jobs")

    assert response.status_code == 200
    assert response.json() == format_jobs_data(process_manager.get_jobs.return_value)


def test_get_jobs_endpoint_returns_404_on_error(client, mocker):
    """Test the jobs endpoint when get_jobs raises an exception."""
    process_manager = mocker.Mock()
    client.app.extra["process_manager"] = process_manager
    process_manager.get_jobs.side_effect = Exception("get_jobs failed")

    response = client.get("/dpr/jobs")

    assert response.status_code == 404
    assert response.json()["detail"] == "get_jobs failed"


def test_delete_job_endpoint_success(client, mocker):
    """Test the delete job endpoint when the job exists."""
    cancel_event = mocker.Mock()
    cancel_event.client = True
    mocker.patch("rs_dpr_service.main.distributed.Event", return_value=cancel_event)
    process_manager = mocker.Mock()
    client.app.extra["process_manager"] = process_manager
    process_manager.get_job.return_value = copy.deepcopy(job_record("job-1"))

    response = client.delete("/dpr/jobs/job-1")

    expected_job = format_job_data(
        {
            **job_record("job-1"),
            "message": "Job job-1 deleted successfully",
        },
    )
    assert response.status_code == 200
    assert response.json() == expected_job
    cancel_event.set.assert_called_once()
    process_manager.delete_job.assert_called_once_with("job-1")


def test_delete_job_endpoint_returns_404_for_unknown_job(client, mocker):
    """Test the delete job endpoint when the job does not exist."""
    cancel_event = mocker.Mock()
    cancel_event.client = False
    mocker.patch("rs_dpr_service.main.distributed.Event", return_value=cancel_event)
    process_manager = mocker.Mock()
    client.app.extra["process_manager"] = process_manager
    process_manager.get_job.side_effect = main_module.JobNotFoundError

    response = client.delete("/dpr/jobs/unknown-job")

    assert response.status_code == 404
    assert response.json()["detail"] == "Job with ID unknown-job not found"
    cancel_event.set.assert_not_called()


def test_variable_processors(client, mocker, monkeypatch):
    """Test that only ceertain processors are exposed."""
    client.app.extra["process_manager"] = mocker.Mock()

    monkeypatch.setenv(
        "DPR_ENABLED_PROCESSORS",
        "conv_safe_zarr,s1_l0,s3_l0",
    )

    reload(main_module)
    response = client.get("/dpr/processes")
    assert response.status_code == 200

    processes = {p["id"] for p in response.json()["processes"]}

    assert processes == {
        "conv_safe_zarr",
        "s1_l0",
        "s3_l0",
    }

    assert "mockup" not in processes
