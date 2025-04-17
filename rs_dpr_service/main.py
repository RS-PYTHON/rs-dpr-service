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
from fastapi import APIRouter, FastAPI
from pygeoapi.api import API
from pygeoapi.process.base import JobNotFoundError
from pygeoapi.process.manager.postgresql import PostgreSQLManager
from pygeoapi.provider.postgresql import get_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base

from rs_dpr_service import opentelemetry

# Construct a sqlalchemy base class for declarative class definitions.
Base = declarative_base()
# Initialize a FastAPI application
app = FastAPI(title="rs-dpr-service", root_path="", debug=True)
router = APIRouter(tags=["DPR service"])

logger = logging.getLogger("my_logger")
logger.setLevel(logging.DEBUG)


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
    yield


# DPR_SERVICE FRONT LOGIC HERE

# DPR_SERVICE FRONT LOGIC HERE


app.include_router(router)
app.router.lifespan_context = app_lifespan
opentelemetry.init_traces(app, "rs.dpr.service")
# Mount pygeoapi endpoints
app.mount(path="/oapi", app=api)
