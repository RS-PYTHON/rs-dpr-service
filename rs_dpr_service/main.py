# Copyright 2024 CS Group
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

"""rs dpr service main module."""
import copy
import os
import pathlib
from contextlib import asynccontextmanager
from datetime import datetime
from string import Template
from time import sleep

import yaml
from fastapi import APIRouter, FastAPI, Path
from pygeoapi.api import API
from pygeoapi.process.base import JobNotFoundError
from pygeoapi.process.manager.postgresql import PostgreSQLManager
from pygeoapi.provider.sql import get_engine  # pylint: disable=no-name-in-module
from sqlalchemy.exc import SQLAlchemyError
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.status import (  # pylint: disable=C0411
    HTTP_200_OK,
    HTTP_201_CREATED,
    HTTP_404_NOT_FOUND,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

from rs_dpr_service.conversion_processor import ConversionProcessor
from rs_dpr_service.jobs_table import Base
from rs_dpr_service.openapi_validation import (
    validate_request,
    validate_response,
)
from rs_dpr_service.processors import (
    GeneralProcessor,
    S1L0Processor,
    S3L0Processor,
)
from rs_dpr_service.utils import init_opentelemetry
from rs_dpr_service.utils.logging import Logging

# flake8: noqa: F401
# DON'T REMOVE (needed for SQLAlchemy)
from . import jobs_table  # pylint: disable=unused-import

# Register all the processors
processors: dict[str, type[GeneralProcessor]] = {
    "S1L0_processor": S1L0Processor,
    "S3L0_processor": S3L0Processor,
    "Conversion_Processor": ConversionProcessor,
}

# Initialize a FastAPI application
app = FastAPI(title="rs-dpr-service", root_path="", debug=True)
router = APIRouter(tags=["DPR service"])

JOB_ATTRS_MAPPING = {"identifier": "jobID"}
OGC_UNCOMPLIANT_JOB_ATTRS = ["_sa_instance_state", "location", "mimetype"]

logger = Logging.default(__name__)


def ogc_error_response(status_code: int, detail: str):
    """Generate an OGC-compliant error response"""
    error_response = {
        "type": f"https://developer.mozilla.org/en/docs/Web/HTTP/Reference/Status/{status_code}",
        "status": status_code,
        "detail": detail,
    }
    return JSONResponse(status_code=status_code, content=error_response)


class DatabaseJobFormatError(Exception):
    """Exception raised when an error occurred during the init of a provider."""


class JobsFormatError(Exception):
    """Exception raised when an error occurred during the init of a provider."""


def get_config_path() -> pathlib.Path:
    """Return the pygeoapi configuration path and set the PYGEOAPI_CONFIG env var accordingly."""
    path = pathlib.Path(__file__).parent.parent / "config" / "geoapi.yaml"
    os.environ["PYGEOAPI_CONFIG"] = str(path)
    return path


def get_config_contents() -> dict:
    """Return the pygeoapi configuration yaml file contents."""
    # Open the configuration file
    with open(get_config_path(), encoding="utf8") as opened:
        contents = opened.read()

        # Replace env vars by their value
        contents = Template(contents).substitute(os.environ)

        # Parse contents as yaml
        return yaml.safe_load(contents)


def init_pygeoapi() -> API:
    """Init pygeoapi"""
    return API(get_config_contents(), "")


api = init_pygeoapi()


# Filelock to be added ?
def init_db(pause: int = 3, timeout: int | None = None) -> PostgreSQLManager:
    """Initialize the PostgreSQL database connection and sets up required table and ENUM type.

    This function constructs the database URL using environment variables for PostgreSQL
    credentials, host, port, and database name. It then creates an SQLAlchemy engine and
    registers the ENUM type JobStatus and the 'job' tables if they don't already exist.

    Environment Variables:
        - POSTGRES_USER: Username for database authentication.
        - POSTGRES_PASSWORD: Password for the database.
        - POSTGRES_HOST: Hostname of the PostgreSQL server.
        - POSTGRES_PORT: Port number of the PostgreSQL server.
        - POSTGRES_DB: Database name.

    Args:
        pause: pause in seconds to wait for the database connection.
        timeout: timeout in seconds to wait for the database connection.

    Returns:
        PostgreSQLManager instance
    """
    manager_def = api.config["manager"]
    if not manager_def or not isinstance(manager_def, dict) or not isinstance(manager_def["connection"], dict):
        message = "Error reading the manager definition for pygeoapi PostgreSQL Manager"
        logger.error(message)
        raise RuntimeError(message)
    connection = manager_def["connection"]

    # Create SQL Alchemy engine
    engine = get_engine(driver_name="postgresql+psycopg2", **connection)

    while True:
        try:
            # This registers the ENUM type and creates the jobs table if they do not exist
            Base.metadata.create_all(bind=engine)
            logger.info(f"Reached {engine.url!r}")
            logger.info("Database table and ENUM type created successfully.")
            break

        # It fails if the database is unreachable. Wait a few seconds and try again.
        except SQLAlchemyError:
            logger.warning(f"Trying to reach {engine.url!r}")

            # Sleep for n seconds and raise exception if timeout is reached.
            if timeout is not None:
                timeout -= pause
                if timeout < 0:
                    raise
            sleep(pause)

    # Initialize PostgreSQLManager with the manager configuration
    return PostgreSQLManager(manager_def)


@asynccontextmanager
async def app_lifespan(fastapi_app: FastAPI):
    """Lifespann app to be implemented with start up / stop logic"""
    logger.info("Starting up the application...")
    # Create jobs table
    process_manager = init_db()

    # This url is needed by the eopf dask scheduler to connect later to this cluster
    os.environ["DASK_GATEWAY_EOPF_ADDRESS"] = os.environ["DASK_GATEWAY__ADDRESS"]

    fastapi_app.extra["process_manager"] = process_manager
    # fastapi_app.extra["db_table"] = db.table("jobs")
    # fastapi_app.extra["dask_cluster"] = cluster

    # Yield control back to the application (this is where the app will run)
    yield

    # Shutdown logic (cleanup)
    logger.info("Shutting down the application...")
    logger.info("Application gracefully stopped...")


# Health check route
@router.get("/_mgmt/ping", include_in_schema=False)
async def ping():
    """Liveliness probe."""
    return JSONResponse(status_code=HTTP_200_OK, content="Healthy")


# Endpoint to return the names of the available processors
@router.get("/dpr/processes")
async def get_processes(request: Request):
    """Returns list of all available processes from config."""

    try:
        processes = {
            "processes": [],
            "links": [
                {"href": str(request.url), "rel": "self", "type": "application/json", "title": "List of processes"},
            ],
        }
        for resource in api.config["resources"]:
            processes["processes"].append(
                {
                    "id": api.config["resources"][resource]["processor"]["name"],
                    "version": "1.0.0",
                },
            )
        validate_response(request, processes)
        return JSONResponse(status_code=HTTP_200_OK, content=processes)

    except Exception as e:  # pylint: disable=W0718
        return ogc_error_response(HTTP_500_INTERNAL_SERVER_ERROR, str(e))


@router.get("/dpr/processes/{resource}")
async def get_resource(request: Request, resource: str):
    """Should return info about a specific resource."""
    with init_opentelemetry.start_span(__name__, "tasktable"):

        if resource_info := next(  # pylint: disable=W0612 # noqa: F841
            (
                api.config["resources"][defined_resource]
                for defined_resource in api.config["resources"]
                if defined_resource == resource
            ),
            None,
        ):
            try:
                data = (await request.json()) or {}
            except Exception:  # pylint: disable=broad-exception-caught
                data = {}
            processor_name = api.config["resources"][resource]["processor"]["name"]
            if processor_name in processors:
                processor = processors[processor_name]
                task_table = await processor(  # type: ignore
                    request,
                    app.extra["process_manager"],
                    # app.extra["dask_cluster"],
                ).get_tasktable(data)

                return JSONResponse(status_code=HTTP_200_OK, content=task_table)
        return ogc_error_response(HTTP_404_NOT_FOUND, f"Resource {resource} not found")


def format_job_data(job: dict):
    """
    Method to apply reformatting on job data to make it compliant with OGC (process) standards
    Args:
        job: information on a specific job to fromat: the job must have the same attributes
        than the columns from the PostgreSql database
    Result:
        reformatted and validated job_data variable to put in the response
    """
    # Check that the input job have the same struture as the jobs contained in the PostgreSQL database
    if "identifier" not in job:
        raise DatabaseJobFormatError(
            """Input job must have the same structure than the jobs stored in the """
            """PostgreSql database: attribute 'identifier' is missing""",
        )
    job_data = copy.deepcopy(job)
    # Rename attribute "identifier" to be compliant with OGC standards
    job_data[JOB_ATTRS_MAPPING["identifier"]] = job_data.pop("identifier")
    # Remove attributes which should not be part of the response
    for attr in OGC_UNCOMPLIANT_JOB_ATTRS:
        if attr in job_data:
            job_data.pop(attr)
    for key, value in job_data.items():
        # Reformat datetime object to string
        if isinstance(value, datetime):
            job_data[key] = value.strftime("%Y-%m-%dT%H:%M:%SZ")
    # Remove "finished" attribute if its value is None
    if "finished" in job_data and job_data.get("finished") is None:
        job_data.pop("finished")
    return job_data


def format_jobs_data(jobs: dict):
    """
    Method validate information on all existing jobs

    Args:
        jobs: information on all existing jobs
    Result:
        reformatted and validated jobs_data variable to provide to the response
    """
    if not isinstance(jobs, dict):
        raise JobsFormatError("Expected a dictionary as input")
    if "jobs" not in jobs:
        raise JobsFormatError("Invalid format for input jobs: missing 'jobs' key")
    jobs_data = copy.deepcopy(jobs)
    # Add "links" mandatory field to the response
    jobs_data.update(
        {
            "links": [
                {
                    "href": "string",
                    "rel": "service",
                    "type": "application/json",
                    "hreflang": "en",
                    "title": "List of jobs",
                },
            ],
        },
    )
    # Remove SQLAlchemy _sa_instance_state objects and convert datetime
    for i, job_data in enumerate(jobs_data["jobs"]):
        jobs_data["jobs"][i] = format_job_data(job_data)
    return jobs_data


# Endpoint to execute the rs-dpr-service process and generate a job ID
@router.post("/dpr/processes/{resource}/execution")
async def execute_process(request: Request, resource: str):  # pylint: disable=unused-argument
    """Used to execute processing jobs."""

    with init_opentelemetry.start_span(__name__, "processor"):

        # check if the input resource exists
        if resource not in api.config["resources"]:
            return ogc_error_response(HTTP_404_NOT_FOUND, f"Process resource '{resource}' not found")

        # Validate request payload
        try:
            valid_body = await validate_request(request)
        except Exception as e:  # pylint: disable=W0718
            # Handle exceptions and return an appropriate error message
            return ogc_error_response(HTTP_500_INTERNAL_SERVER_ERROR, str(e))

        processor_name = api.config["resources"][resource]["processor"]["name"]
        if processor_name in processors:
            processor = processors[processor_name]
            _, dpr_status = await processor(  # type: ignore
                request,
                app.extra["process_manager"],
                # app.extra["dask_cluster"],
            ).execute(valid_body)

            # Get identifier of the current job
            status_dict = {
                "accepted": HTTP_201_CREATED,
                "running": HTTP_201_CREATED,
                "successful": HTTP_201_CREATED,
                "failed": HTTP_500_INTERNAL_SERVER_ERROR,
                "dismissed": HTTP_500_INTERNAL_SERVER_ERROR,
            }
            id_key = [status for status in status_dict if status in dpr_status][0]
            formatted_job_data = format_job_data(app.extra["process_manager"].get_job(dpr_status[id_key]))
            validate_response(request, formatted_job_data, HTTP_201_CREATED)
            return JSONResponse(status_code=HTTP_201_CREATED, content=formatted_job_data)
        return ogc_error_response(HTTP_404_NOT_FOUND, f"Processor '{processor_name}' not found")


# Endpoint to return the list of jobs
@router.get("/dpr/jobs")
async def get_jobs_list(request: Request):
    """Returns all jobs from database."""
    try:
        formatted_jobs_data = format_jobs_data(app.extra["process_manager"].get_jobs())
        validate_response(request, formatted_jobs_data)
        return JSONResponse(status_code=HTTP_200_OK, content=formatted_jobs_data)
    except Exception as e:  # pylint: disable=W0718
        return ogc_error_response(HTTP_404_NOT_FOUND, str(e))


# Endpoint to get the status of a job by job_id
@router.get("/dpr/jobs/{job_id}")
async def get_job_status_endpoint(request: Request, job_id: str = Path(..., title="The ID of the job")):
    """Used to get status of processing job."""
    try:
        job = app.extra["process_manager"].get_job(job_id)
    except JobNotFoundError:  # pylint: disable=W0718
        # Handle case when job_id is not found
        return ogc_error_response(HTTP_404_NOT_FOUND, f"Job with ID {job_id} not found")

    try:
        formatted_job_data = format_job_data(job)
        validate_response(request, formatted_job_data)
        return JSONResponse(status_code=HTTP_200_OK, content=formatted_job_data)
    except Exception as e:  # pylint: disable=W0718
        return ogc_error_response(HTTP_500_INTERNAL_SERVER_ERROR, str(e))


# DPR_SERVICE FRONT LOGIC HERE


app.include_router(router)
app.router.lifespan_context = app_lifespan  # type: ignore
init_opentelemetry.init_traces(app, "rs.dpr.service")
# Mount pygeoapi endpoints
app.mount(path="/oapi", app=api)
