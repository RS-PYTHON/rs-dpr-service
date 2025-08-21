"""Test endpoints"""

import pytest
from fastapi import FastAPI
from datetime import datetime
import copy

from rs_dpr_service.main import app
from rs_dpr_service.main import (
    app_lifespan,
    format_job_data,
    format_jobs_data,
    init_pygeoapi,
)
from starlette.status import (
    HTTP_200_OK,
    HTTP_404_NOT_FOUND,
    HTTP_500_INTERNAL_SERVER_ERROR
)


@pytest.mark.asyncio
async def test_processes(
    client,
    predefined_config,
    mocker
):
    """
    Test the /processes endpoint for retrieving a list of available processors.

    This test verifies that the processors returned by the /processes endpoint
    match those defined in the provided configuration. It ensures that the
    API returns the expected processors correctly.

     Args:
        client: A test client for making requests to the FastAPI application.
        predefined_config (dict): A configuration dictionary containing predefined
            resources with their associated processors.

    Assertions:
        - Asserts that the list of processors returned from the API matches
        the list defined in the predefined configuration.
    """

    response = client.get("/dpr/processes")

    assert response.status_code == HTTP_200_OK
    input_processors = [resource["processor"]["name"] for resource in predefined_config["resources"].values()]
    # Extract processors from the output
    output_processors = [process["id"] for process in response.json()["processes"]]
    # Assert that both lists of processors match
    assert sorted(input_processors) == sorted(output_processors), "Processors do not match!"

    # ----- Mock api.config to send a list of resources with an incorrect format, check that the right
    # # validation exception is raised
    mock_resources = {
        "mock_resource_1": {
            "type": "process",
            "processor": {"name": {"wrong_processor_name_format": "wrong_processor_name_format"}},
        },
        "mock_resource_2": {
            "type": "process",
            "processor": {"name": {"wrong_processor_name_format": "wrong_processor_name_format"}},
        },
    }
    mocker.patch.dict("rs_dpr_service.main.api.config", {"resources": mock_resources})
    response = client.get("/dpr/processes")
    assert response.status_code == HTTP_500_INTERNAL_SERVER_ERROR
    assert "is not of type 'string'" in response.json()["detail"]


@pytest.mark.asyncio
async def test_get_jobs_endpoint(
    mocker,
    client
):
    """
    Test the GET /jobs endpoint for retrieving job listings.

    This test verifies the behavior of the /jobs endpoint when jobs are present
    in the postgres jobs table. It checks that the API correctly returns the list of
    jobs when available, as well as the handling of cases where no jobs exist.

    Args:
        mocker: A mocker object used to create mocks and patches for testing.
        staging_client: A test client for making requests to the FastAPI application.

    Assertions:
        - Asserts that the response status code is 200 and the returned job list
          matches the simulated job data when jobs are present in the database.
    """

    mock_jobs = [
		{
			"type": "process",
			"status": "failed",
			"message": "No dask cluster named 'dask-eopf-mockup' was found.",
			"progress": 0,
			"processID": "dpr-service",
			"created": datetime(2025,8,19, 12,51,14),
			"started": datetime(2025,8,19, 12,51,14),
			"updated": datetime(2025,8,19, 12,51,14),
			"jobID": "0509e017-ca9b-409d-ab0b-55dab51689f6"
		},
		{
			"type": "process",
			"status": "failed",
			"message": "No dask cluster named 'dask-eopf-mockup' was found.",
			"progress": 0,
			"processID": "dpr-service",
			"created": datetime(2025,8,19, 9,18,9),
			"started": datetime(2025,8,19, 9,18,9),
			"updated": datetime(2025,8,19, 9,18,9),
			"jobID": "a02205b9-8b38-44e1-b655-8b1d5447d371"
		}
    ]

    mock_jobs_result = [format_job_data(x) for x in mock_jobs]
    links = [
        {"href": "string", "rel": "service", "type": "application/json", "hreflang": "en", "title": "List of jobs"},
    ]

     # ----- Mock app.extra with some jobs from the database mock to ensure 'db_table' exists
    mock_db_table = mocker.MagicMock()

    # Simulate postgres returning jobs
    mock_db_table.get_jobs.return_value = {"jobs": mock_jobs, "numberMatched": 2}
    # Patch app.extra with the mock db_table
    # Ensure app.extra contains all necessary attributes at once
    client.app.extra["process_manager"] = mock_db_table

    # Call the API
    response = client.get("/dpr/jobs")

    # Assertions
    assert response.status_code == HTTP_200_OK
    # Check if the returned data matches the mocked jobs
    assert response.json() == {"jobs": list(mock_jobs_result), "numberMatched": 2, "links": links}

     # ----- Mock with an empty db
    mock_db_table.get_jobs.return_value = {"jobs": [], "numberMatched": 0}
    # Patch app.extra with the mock db_table
    client.app.extra["process_manager"] = mock_db_table
    response = client.get("/dpr/jobs")
    assert response.status_code == HTTP_200_OK
    # Check if the returned data matches 0 jobs
    assert response.json() == {"jobs": [], "numberMatched": 0, "links": links}

    # ----- Check that a validation exception is returned if one of the job from the response doesn't have
    # the required "type" property (and thus is not ogc compliant)
    wrong_ogc_mock_jobs = copy.deepcopy(mock_jobs)
    # Remove required ogc attribute "type"
    wrong_ogc_mock_jobs[0].pop("type")
    mock_db_table.get_jobs.return_value = {"jobs": list(wrong_ogc_mock_jobs), "numberMatched": 2}
    client.app.extra["process_manager"] = mock_db_table
    response = client.get("/dpr/jobs")
    assert response.status_code == HTTP_404_NOT_FOUND
    assert "'type' is a required property" in response.json()["detail"]

    # ----- Check that a validation exception is returned if the response doesn't have the required "links" property
    # (and thus is not ogc compliant)
    mock_formatted_jobs = format_jobs_data({"jobs": mock_jobs, "numberMatched": 2})
    mock_formatted_jobs.pop("links")
    mocker.patch("rs_dpr_service.main.format_jobs_data", return_value=mock_formatted_jobs)
    client.app.extra["process_manager"] = mock_db_table
    response = client.get("/dpr/jobs")
    assert response.status_code == HTTP_404_NOT_FOUND
    assert "'links' is a required property" in response.json()["detail"]

    # ----- Simulate an error response compliant with ogc
    ogc_error_example = {
        "type": "https://developer.mozilla.org/en/docs/Web/HTTP/Reference/Status/404",
        "status": 404,
        "detail": "get_jobs failed",
    }

    mocker.patch("rs_dpr_service.main.format_jobs_data", side_effect=Exception("get_jobs failed"))
    response = client.get("/dpr/jobs")
    assert response.status_code == HTTP_404_NOT_FOUND
    assert response.json() == ogc_error_example