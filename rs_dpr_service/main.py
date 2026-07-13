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

"""rs dpr service main module."""

import asyncio
import copy
import logging
import os
import pathlib
from contextlib import asynccontextmanager
from datetime import datetime
from string import Template
from time import sleep
from typing import Annotated

import distributed
import yaml
from fastapi import APIRouter, FastAPI, Path
from opentelemetry.trace import StatusCode
from pygeoapi.api import API
from pygeoapi.process.base import JobNotFoundError
from pygeoapi.process.manager.postgresql import PostgreSQLManager
from pygeoapi.provider.sql import get_engine  # pylint: disable=no-name-in-module
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.status import (  # pylint: disable=C0411
    HTTP_200_OK,
    HTTP_201_CREATED,
    HTTP_404_NOT_FOUND,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

from rs_dpr_service.dask.call_dask import ClusterInfo
from rs_dpr_service.jobs_table import Base
from rs_dpr_service.openapi_validation import (
    validate_request,
    validate_response,
)
from rs_dpr_service.processors.conversion_processor import ConversionProcessor
from rs_dpr_service.processors.eopf_processors import (
    MockupProcessor,
    S1ARDProcessor,
    S1L0Processor,
    S3L0Processor,
    S3L1OLCIProcessor,
    S3L2OLCIProcessor,
)
from rs_dpr_service.processors.generic_processor import GenericProcessor
from rs_dpr_service.utils.init_opentelemetry import (
    init_traces,
    record_error,
    start_span,
)
from rs_dpr_service.utils.logging import JobLogHandler, Logging
from rs_dpr_service.utils.middlewares import (
    HandleExceptionsMiddleware,
    HealthMiddleware,
)
from rs_dpr_service.utils.settings import CANCEL_JOB

# flake8: noqa: F401
# DON'T REMOVE (needed for SQLAlchemy)
from . import jobs_table  # pylint: disable=unused-import

# Register all the processors.
# Keys are defined in rs-dpr-service/config/geoapi.yaml
# Values are the Python classes.
processor_types: dict[str, type[GenericProcessor]] = {
    "conv_safe_zarr": ConversionProcessor,
    "mockup": MockupProcessor,
    "s1_l0": S1L0Processor,
    "s3_l0": S3L0Processor,
    "s1_ard": S1ARDProcessor,
    "s3_l1olci": S3L1OLCIProcessor,
    "s3_l2olci": S3L2OLCIProcessor,
}

# Initialize a FastAPI application
app = FastAPI(title="rs-dpr-service", root_path="", debug=True)
router = APIRouter(tags=["Processing service"])

JOB_ATTRS_MAPPING = {"identifier": "jobID"}
OGC_UNCOMPLIANT_JOB_ATTRS = ["_sa_instance_state", "location", "mimetype"]

logger = Logging.default(__name__)


job_log_handler = JobLogHandler()
job_log_handler.setLevel(logging.DEBUG)
# Attach to the logger where dask_client.forward_logging sends worker logs
logging.getLogger("rs_dpr_service.dask.call_dask").addHandler(job_log_handler)


class DatabaseJobFormatError(Exception):
    """Exception raised when an error occurred during the init of a provider."""


class JobsFormatError(Exception):
    """Exception raised when an error occurred during the init of a provider."""


# Add middlewares. When sending a request, the middleware order must be:
# Health -> HandleExceptions -> [any other middlewares ...]
# Then after processing the request, the response is sent in the opposite order:
# [any other middlewares ...] -> HandleExceptions -> Health

# Catch all exceptions and return a JSONResponse
app.add_middleware(HandleExceptionsMiddleware, rfc7807=True)
HandleExceptionsMiddleware.disable_default_exception_handler(app)

# More responsive /health and /ping endpoints
app.add_middleware(HealthMiddleware)


def get_enabled_processors() -> set[str] | None:
    """Return the desired processors. These are set in DPR_ENABLED_PROCESSORS by deployment in rs-helm."""
    raw = os.environ.get("DPR_ENABLED_PROCESSORS")
    if raw is None:
        return None
    return {item.strip() for item in raw.split(",") if item.strip()}


def filter_enabled_resources(config: dict) -> dict:
    """Return the usual list of processors in local mode, and a specific one on cluster."""

    enabled = get_enabled_processors()

    # For localmode use the default configuration
    if enabled is None:
        return config

    config["resources"] = {
        resource_name: resource_config
        for resource_name, resource_config in config.get("resources", {}).items()
        if resource_config.get("processor", {}).get("name") in enabled
    }
    return config


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
        config = yaml.safe_load(contents)
        return filter_enabled_resources(config)


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

    fastapi_app.extra["process_manager"] = process_manager
    # fastapi_app.extra["db_table"] = db.table("jobs")
    # fastapi_app.extra["dask_cluster"] = cluster

    # Yield control back to the application (this is where the app will run)
    yield

    # Shutdown logic (cleanup)
    logger.info("Shutting down the application...")
    logger.info("Application gracefully stopped...")


def build_cluster_info(data: dict) -> ClusterInfo:
    """This function handles missing parameters from request, hence properly creates a ClusterInfo object."""
    jupyter_token = data.get("jupyter_token")
    cluster_label = data.get("cluster_label")
    cluster_instance = data.get("cluster_instance", "")

    if jupyter_token is None or cluster_label is None:
        raise HTTPException(status_code=400, detail="Missing required fields: jupyter_token or cluster_label")

    return ClusterInfo(
        jupyter_token=jupyter_token,
        cluster_label=cluster_label,
        cluster_instance=cluster_instance,
    )


# Endpoint to return the names of the available processors
@router.get("/dpr/processes")
async def get_processes(request: Request):
    """Returns list of all available processes from config."""
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


@router.get("/dpr/processes/{resource}")
async def get_resource(request: Request, resource: str):
    """Should return info about a specific resource."""
    with start_span(__name__, "tasktable"):

        # Check that the input resource exists
        if resource not in api.config["resources"]:
            return JSONResponse(status_code=HTTP_404_NOT_FOUND, content=f"Process {resource!r} not found")

        cluster_info = build_cluster_info(dict(request.query_params))

        processor_name = api.config["resources"][resource]["processor"]["name"]
        if processor_name in processor_types:
            processor_type = processor_types[processor_name]
            task_table = await processor_type(app.extra["process_manager"], cluster_info).get_tasktable()
            return JSONResponse(status_code=HTTP_200_OK, content=task_table)


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

    # Validate request payload. This will create the 'http receive' fastapi span
    valid_body = await validate_request(request)

    # Run business logic in our own span
    with start_span(__name__, f"execute_processor_{resource}") as span:
        try:
            # Check that the input resource exists
            if resource not in api.config["resources"]:
                err_msg = f"Process {resource!r} not found"
                span.set_status(StatusCode.ERROR, err_msg)
                return JSONResponse(status_code=HTTP_404_NOT_FOUND, content=err_msg)

            cluster_info = build_cluster_info(valid_body)

            processor_name = api.config["resources"][resource]["processor"]["name"]
            if processor_name in processor_types:
                processor_type = processor_types[processor_name]
                # Asynchronously execute the dpr process in the dask cluster
                _, dpr_status = await processor_type(app.extra["process_manager"], cluster_info).execute(
                    valid_body,
                )  # type: ignore

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
                span.set_status(StatusCode.OK, str(formatted_job_data))
                return JSONResponse(status_code=HTTP_201_CREATED, content=formatted_job_data)

            err_msg = f"Processor {processor_name!r} not found"
            span.set_status(StatusCode.ERROR, err_msg)
            return JSONResponse(status_code=HTTP_404_NOT_FOUND, content=err_msg)

        except Exception as e:
            record_error(span, e)
            raise


# Endpoint to stream the logs of a job by job_id
@router.get("/dpr/jobs/{job_id}/logs")
async def get_job_logs_endpoint(request: Request, job_id=Annotated[str, Path(..., title="The ID of the job")]):
    """Used to stream logs of a processing job."""

    # Verify job exists
    try:
        app.extra["process_manager"].get_job(job_id)
    except JobNotFoundError:
        return JSONResponse(status_code=HTTP_404_NOT_FOUND, content=f"Job with ID {job_id} not found")

    queue: asyncio.Queue[str] = asyncio.Queue()
    job_log_handler.queues[job_id].append(queue)

    async def log_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break

                try:
                    # Wait for log message
                    msg = await asyncio.wait_for(queue.get(), timeout=2.0)
                    yield f"data: {msg}\n\n"
                except TimeoutError:
                    # Check if job is still running
                    try:
                        # Send a keepalive message to keep the connection alive (prevents proxy timeouts)
                        yield ": keepalive\n\n"
                        job_status = app.extra["process_manager"].get_job(job_id)
                        if job_status.get("status") in ["successful", "failed", "dismissed"]:
                            # job finished. drain remaining queue items (if any)
                            while not queue.empty():
                                yield f"data: {queue.get_nowait()}\n\n"
                            break
                    except Exception as err:  # pylint: disable=broad-exception-caught
                        # Do not break the stream if there's a transient DB error
                        logger.warning(f"Error checking job status during log stream: {err}")
                        continue
        finally:
            job_log_handler.queues[job_id].remove(queue)
            if not job_log_handler.queues[job_id]:
                del job_log_handler.queues[job_id]

    return StreamingResponse(log_generator(), media_type="text/event-stream")


# Endpoint to get the status of a job by job_id
@router.get("/dpr/jobs/{job_id}")
async def get_job_status_endpoint(request: Request, job_id=Annotated[str, Path(..., title="The ID of the job")]):
    """Used to get status of processing job."""
    try:
        job = app.extra["process_manager"].get_job(job_id)
    except JobNotFoundError:  # pylint: disable=W0718
        # Handle case when job_id is not found
        return JSONResponse(status_code=HTTP_404_NOT_FOUND, content=f"Job with ID {job_id} not found")

    formatted_job_data = format_job_data(job)
    validate_response(request, formatted_job_data)
    return JSONResponse(status_code=HTTP_200_OK, content=formatted_job_data)


# Endpoint to return the list of jobs
@router.get("/dpr/jobs")
async def get_jobs_endpoint(request: Request):
    """Returns the status of all jobs from database."""
    try:
        # Generate an output conform to OGC process specifications
        formatted_jobs_data = format_jobs_data(app.extra["process_manager"].get_jobs())
        validate_response(request, formatted_jobs_data)
        return JSONResponse(status_code=HTTP_200_OK, content=formatted_jobs_data)
    except Exception as e:  # pylint: disable=W0718
        # Handle exceptions and return an appropriate error message
        return JSONResponse(status_code=HTTP_404_NOT_FOUND, content=str(e))


@router.delete("/dpr/jobs/{job_id}")
async def delete_job_endpoint(request: Request, job_id=Annotated[str, Path(..., title="The ID of the job to delete")]):
    """Deletes a specific job from the database."""

    # Send a dask distributed event for the cancellation of the job
    cancel_event = distributed.Event(CANCEL_JOB.format(job_id=job_id))
    if cancel_event.client:
        cancel_event.set()

    try:
        job = app.extra["process_manager"].get_job(job_id)
    # Handle case when job_id is not found
    except JobNotFoundError:  # pylint: disable=W0718
        return JSONResponse(status_code=HTTP_404_NOT_FOUND, content=f"Job with ID {job_id} not found")

    app.extra["process_manager"].delete_job(job_id)

    # Create job response with a status message to confirm the job deletion
    job["message"] = f"Job {job_id} deleted successfully"
    formatted_job_data = format_job_data(job)
    validate_response(request, formatted_job_data)
    return JSONResponse(status_code=HTTP_200_OK, content=formatted_job_data)


# DPR_SERVICE FRONT LOGIC HERE


app.include_router(router)
app.router.lifespan_context = app_lifespan  # type: ignore
init_traces(app, "rs.dpr.service")
# Mount pygeoapi endpoints
app.mount(path="/oapi", app=api)
