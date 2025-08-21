from fastapi.testclient import TestClient
import pytest
import yaml
import os
import os.path as osp
from pathlib import Path
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
