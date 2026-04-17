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

import pytest
from starlette.exceptions import HTTPException as StarletteHTTPException

from rs_dpr_service.main import (
    ClusterInfo,
    DatabaseJobFormatError,
    JobsFormatError,
    build_cluster_info,
    format_job_data,
    format_jobs_data,
)


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


def test_format_job_data_missing_identifier():
    """Test that format_job_data raises when identifier is missing."""
    with pytest.raises(DatabaseJobFormatError) as exc:
        format_job_data({"status": "running"})

    assert "attribute 'identifier' is missing" in str(exc.value)
