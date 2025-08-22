# Copyright 2025 CS Group
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
https://docs.pytest.org/en/6.2.x/fixture.html#conftest-py-sharing-fixtures-across-multiple-files

The conftest.py file serves as a means of providing fixtures for an entire directory.
Fixtures defined in a conftest.py can be used by any test in that package without needing to import them
(pytest will automatically discover them).
"""

import os
import os.path as osp
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from rs_dpr_service.main import app

RESOURCES_FOLDER = Path(osp.realpath(osp.dirname(__file__))).parent / "config"


@pytest.fixture(name="client")
def client_(mocker):
    """init fastapi client app."""
    # Test the FastAPI application, opens the database session
    mocker.patch("rs_dpr_service.main.init_db", return_value=None)
    mocker.patch("rs_dpr_service.main.PostgreSQLManager", return_value=mocker.Mock())
    with TestClient(app) as client:

        yield client

        os.environ["RSPY_LOCAL_MODE"] = "1"


@pytest.fixture(name="geoapi_cfg")
def geoapi_cfg_() -> Path:
    """Return pygeoapi config file path"""
    return RESOURCES_FOLDER / "geoapi.yaml"


@pytest.fixture(name="predefined_config")
def config_(geoapi_cfg):
    """Fixture for pygeoapi yaml config"""
    with open(geoapi_cfg, encoding="utf-8") as yaml_file:
        return yaml.safe_load(yaml_file)
