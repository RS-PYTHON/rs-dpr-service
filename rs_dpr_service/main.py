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

import logging
import os
import pathlib
from contextlib import asynccontextmanager
from string import Template
from time import sleep

import yaml

# from dask.distributed import LocalCluster
from fastapi import APIRouter, FastAPI, HTTPException
from pygeoapi.api import API
from pygeoapi.process.base import JobNotFoundError
from pygeoapi.process.manager.postgresql import PostgreSQLManager
from pygeoapi.provider.postgresql import get_engine
from sqlalchemy.exc import SQLAlchemyError
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.status import (  # pylint: disable=C0411
    HTTP_200_OK,
    HTTP_201_CREATED,
    HTTP_404_NOT_FOUND,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

from rs_dpr_service import opentelemetry
from rs_dpr_service.jobs_table import Base
from rs_dpr_service.processors import processors

# DON'T REMOVE (needed for SQLAlchemy)


# Initialize a FastAPI application
app = FastAPI(title="rs-dpr-service", root_path="", debug=True)
router = APIRouter(tags=["DPR service"])

logger = logging.getLogger("my_logger")
logger.setLevel(logging.DEBUG)


def env_bool(var: str, default: bool) -> bool:
    """
    Return True if an environemnt variable is set to 1, true or yes (case insensitive).
    Return False if set to 0, false or no (case insensitive).
    Return the default value if not set or set to a different value.
    """
    val = os.getenv(var, str(default)).lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    if val in ("n", "no", "f", "false", "off", "0"):
        return False
    return default


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
        # logger.error(message)
        raise RuntimeError(message)
    connection = manager_def["connection"]

    # Create SQL Alchemy engine
    engine = get_engine(**connection)

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
    fastapi_app.extra["local_mode"] = env_bool("RSPY_LOCAL_MODE", default=False)
    # There are 2 containers / pods that may be used:
    # - one with the image that has the real eopf processor
    # - one with the image that has the mockup eopf processor
    # Set by default the env variables for the dask cluster name that will select one of
    # these 2 containers / pods to the one with the real processor
    # Later on, the user that requests one of the endpoints
    # - /dpr/processes/{resource}/execution
    # - /dpr/processes/{resource}
    # may add in the content the following param:
    # "use_mockup": True
    # and the env variables will be changed
    os.environ["DASK_CLUSTER_EOPF_NAME"] = os.environ["RSPY_DASK_DPR_SERVICE_CLUSTER_NAME"]
    os.environ["DASK_GATEWAY_EOPF_ADDRESS"] = os.environ["DASK_GATEWAY__ADDRESS"]

    fastapi_app.extra["process_manager"] = process_manager
    # fastapi_app.extra["db_table"] = db.table("jobs")
    # fastapi_app.extra["dask_cluster"] = cluster
    # token refereshment logic

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


# Endpoint to get the status of a job by job_id
@router.get("/dpr/jobs/{job_id}")
async def get_job_status_endpoint(request: Request, job_id: str):  # pylint: disable=W0613
    """Used to get status of processing job."""
    try:
        job = app.extra["process_manager"].get_job(job_id)
        pretty_job = {"message": job["message"], "status": job["status"]}
        return JSONResponse(status_code=HTTP_200_OK, content=pretty_job)
    except JobNotFoundError:  # pylint: disable=W0718
        # Handle case when job_id is not found
        return HTTPException(HTTP_404_NOT_FOUND, f"Job with ID {job_id} not found")


# Endpoint to execute the rs-dpr-service process and generate a job ID
@router.post("/dpr/processes/{resource}/execution")
async def execute_process(request: Request, resource: str):  # pylint: disable=unused-argument
    """Used to execute processing jobs."""
    data = await request.json()
    # check if the input resource exists
    if resource not in api.config["resources"]:
        return HTTPException(HTTP_404_NOT_FOUND, f"Process resource '{resource}' not found")

    processor_name = api.config["resources"][resource]["processor"]["name"]
    if processor_name in processors:
        processor = processors[processor_name]
        _, dpr_status = await processor(  # type: ignore
            request,
            app.extra["process_manager"],
            # app.extra["dask_cluster"],
        ).execute(data)

        # Get identifier of the current job
        status_dict = {
            "accepted": HTTP_201_CREATED,
            "running": HTTP_201_CREATED,
            "successful": HTTP_201_CREATED,
            "failed": HTTP_500_INTERNAL_SERVER_ERROR,
            "dismissed": HTTP_500_INTERNAL_SERVER_ERROR,
        }
        id_key = [status for status in status_dict if status in dpr_status][0]
        job = app.extra["process_manager"].get_job(dpr_status[id_key])
        return JSONResponse(status_code=HTTP_201_CREATED, content=str(job))
    return HTTPException(HTTP_404_NOT_FOUND, f"Processor '{processor_name}' not found")


@router.get("/dpr/processes/{resource}")
async def get_resource(request: Request, resource: str):
    """Should return info about a specific resource."""
    if resource_info := next(  # pylint: disable=W0612 # noqa: F841
        (
            api.config["resources"][defined_resource]
            for defined_resource in api.config["resources"]
            if defined_resource == resource
        ),
        None,
    ):
        try:
            data = await request.json()
        except Exception:  # pylint: disable=broad-exception-caught
            data = None
        processor_name = api.config["resources"][resource]["processor"]["name"]
        if processor_name in processors:
            processor = processors[processor_name]
            task_table = await processor(  # type: ignore
                request,
                app.extra["process_manager"],
                # app.extra["dask_cluster"],
            ).get_tasktable(data)

            return JSONResponse(status_code=HTTP_200_OK, content=task_table)
    return HTTPException(HTTP_404_NOT_FOUND, f"Resource {resource} not found")


if env_bool("RSPY_LOCAL_MODE", default=False):

    @router.post("/dpr_service/dask/auth", include_in_schema=False)
    async def dask_auth(local_dask_username: str, local_dask_password: str):
        """Set dask cluster authentication, only in local mode."""
        os.environ["LOCAL_DASK_USERNAME"] = local_dask_username
        os.environ["LOCAL_DASK_PASSWORD"] = local_dask_password


# DPR_SERVICE FRONT LOGIC HERE


app.include_router(router)
app.router.lifespan_context = app_lifespan  # type: ignore
opentelemetry.init_traces(app, "rs.dpr.service")
# Mount pygeoapi endpoints
app.mount(path="/oapi", app=api)
