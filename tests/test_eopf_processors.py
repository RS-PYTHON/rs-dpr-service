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

"""Tests for rs_dpr_service.processors.eopf_processors."""

import pytest

from rs_dpr_service.dask.call_dask import ClusterInfo
from rs_dpr_service.processors.eopf_processors import (
    MockupProcessor,
    S1ARDProcessor,
    S1L0Processor,
    S3L0Processor,
    S3L1OLCIProcessor,
    S3L2OLCIProcessor,
)
from rs_dpr_service.processors.generic_processor import GenericProcessor


@pytest.mark.parametrize(
    ("processor_cls", "tasktable_module", "tasktable_class", "use_mockup"),
    [
        (MockupProcessor, "", "", True),
        (S1L0Processor, "l0.s1.s1_l0_processor", "S1L0Processor", False),
        (S3L0Processor, "l0.s3.s3_l0_processor", "S3L0Processor", False),
        (S1ARDProcessor, "s1_l12_rp.computing.ard_processing_units", "Calibration", False),
        (S3L1OLCIProcessor, "s3olci.s3_ol1.ol1_processor", "OL1Processor", False),
        (S3L2OLCIProcessor, "s3olci.s3_ol2.ol2_processor", "OL2Processor", False),
    ],
)
def test_eopf_processors_initialize_expected_generic_processor_configuration(
    mocker,
    monkeypatch,
    processor_cls,
    tasktable_module,
    tasktable_class,
    use_mockup,
):
    """Test each EOPF processor initializes the expected generic processor configuration."""
    # GenericProcessor creates a cluster handler, so keep the Dask gateway env available.
    monkeypatch.setenv("DASK_GATEWAY_ADDRESS", "http://dask-gateway.test")

    db_process_manager = mocker.Mock()
    cluster_info = ClusterInfo(jupyter_token="token", cluster_label="dask-l0")  # nosec B106

    processor = processor_cls(db_process_manager=db_process_manager, cluster_info=cluster_info)

    assert isinstance(processor, GenericProcessor)
    assert processor.tasktable_module == tasktable_module
    assert processor.tasktable_class == tasktable_class
    assert processor.use_mockup is use_mockup
    assert processor.cluster_handler.cluster_info is cluster_info
    assert processor.cluster_handler.cluster_address == "http://dask-gateway.test"
    db_process_manager.add_job.assert_called_once()


@pytest.mark.parametrize(
    ("processor_cls", "expected_filename"),
    [
        (MockupProcessor, "tasktable.json"),
        (S1L0Processor, "TaskTable_S1_L0_generated_by_rs_python_v1.json"),
        (S3L0Processor, "TaskTable_S3_L0_generated_by_rs_python_v1.json"),
        (S1ARDProcessor, "TaskTable_S1_ARD_generated_by_rs_python_v1.json"),
        (S3L1OLCIProcessor, "TaskTable_S3_L1OLCI_generated_by_rs_python_v1.json"),
        (S3L2OLCIProcessor, "TaskTable_S3_L2OLCI_generated_by_rs_python_v1.json"),
    ],
)
@pytest.mark.asyncio
async def test_eopf_processors_get_tasktable_loads_the_expected_tasktable_file(
    mocker,
    monkeypatch,
    processor_cls,
    expected_filename,
):
    """Test each EOPF processor get_tasktable() loads the expected tasktable file."""
    # Instantiation still goes through GenericProcessor, so keep the Dask gateway env available.
    monkeypatch.setenv("DASK_GATEWAY_ADDRESS", "http://dask-gateway.test")

    expected_tasktable = {"filename": expected_filename}
    load_tasktable = mocker.patch(
        "rs_dpr_service.processors.eopf_processors._load_tasktable",
        return_value=expected_tasktable,
    )

    processor = processor_cls(
        db_process_manager=mocker.Mock(),
        cluster_info=ClusterInfo(jupyter_token="token", cluster_label="dask-l0"),  # nosec B106
    )

    result = await processor.get_tasktable()

    load_tasktable.assert_called_once_with(expected_filename)
    assert result == expected_tasktable
