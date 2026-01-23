# Copyright 2023-2025 Airbus, CS Group
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

"""Module used to configure pytests."""

# Ignore not-at-top level import errors

# pylint: disable=C0413, ungrouped-imports, unused-argument
# flake8: noqa: F402

import asyncio
import os
import os.path as osp
import threading
from datetime import datetime
from importlib import reload
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

# These env vars are mandatory before importing the main module
for envvar in "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB":
    os.environ[envvar] = ""

from rs_dpr_service.main import app


@pytest.fixture(name="client")
def client_(mocker):
    """init fastapi client app."""
    # Test the FastAPI application, opens the database session
    mocker.patch("rs_dpr_service.main.init_db", return_value=None)
    mocker.patch("rs_dpr_service.main.PostgreSQLManager", return_value=mocker.Mock())
    with TestClient(app) as client:

        yield client
