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
Implementation of EOPF processors based on GenericProcessor.
Processors: S1L0, S3L0, S1ARD.
"""

import json
from pathlib import Path

from pygeoapi.process.manager.postgresql import PostgreSQLManager

from rs_dpr_service.dask.call_dask import ClusterInfo
from rs_dpr_service.processors.generic_processor import GenericProcessor

CONFIG_DIR = Path(__file__).parent.parent.parent / "config"


def _load_tasktable(filename: str) -> dict:
    with open(CONFIG_DIR / filename, encoding="utf-8") as f:
        return json.load(f)


class MockupProcessor(GenericProcessor):
    """Mockup Processor implementation"""

    def __init__(self, db_process_manager: PostgreSQLManager, cluster_info: ClusterInfo):
        """
        Initialize S1L0Processor
        """
        super().__init__(
            db_process_manager=db_process_manager,
            cluster_info=cluster_info,
            local_mode_address="DASK_GATEWAY_EOPF_MOCKUP_ADDRESS",
            tasktable_module="",
            tasktable_class="",
        )
        self.use_mockup = True

    async def get_tasktable(self):
        """Return the EOPF tasktable"""
        return _load_tasktable("tasktable.json")


class S1L0Processor(GenericProcessor):
    """S1L0 Processor implementation"""

    def __init__(self, db_process_manager: PostgreSQLManager, cluster_info: ClusterInfo):
        """
        Initialize S1L0Processor
        """
        super().__init__(
            db_process_manager=db_process_manager,
            cluster_info=cluster_info,
            local_mode_address="DASK_GATEWAY_L0_ADDRESS",
            tasktable_module="l0.s1.s1_l0_processor",
            tasktable_class="S1L0Processor",
        )

    async def get_tasktable(self):
        """Return the EOPF tasktable"""
        return _load_tasktable("TaskTable_S1_L0_generated_by_rs_python_v1.json")


class S3L0Processor(GenericProcessor):
    """S3L0 Processor implementation"""

    def __init__(self, db_process_manager: PostgreSQLManager, cluster_info: ClusterInfo):
        """
        Initialize S3L0Processor
        """
        super().__init__(
            db_process_manager=db_process_manager,
            cluster_info=cluster_info,
            local_mode_address="DASK_GATEWAY_L0_ADDRESS",
            tasktable_module="l0.s3.s3_l0_processor",
            tasktable_class="S3L0Processor",
        )

    async def get_tasktable(self):
        """Return the EOPF tasktable"""
        return _load_tasktable("TaskTable_S3_L0_generated_by_rs_python_v1.json")


class S1ARDProcessor(GenericProcessor):
    """S1ARD Processor implementation"""

    def __init__(self, db_process_manager: PostgreSQLManager, cluster_info: ClusterInfo):
        """
        Initialize S1ARDProcessor
        """
        super().__init__(
            db_process_manager=db_process_manager,
            cluster_info=cluster_info,
            local_mode_address="DASK_GATEWAY_S1ARD_ADDRESS",
            tasktable_module="s1_l12_rp.computing.ard_processing_units",
            # NOTE: not implemented for now... and maybe we should be able to return the
            # tasktable for each different processing: Calibration, ReferenceDEM, ReferenceGeometry, ...
            tasktable_class="Calibration",
        )

    async def get_tasktable(self):
        """Return the EOPF tasktable"""
        return _load_tasktable("TaskTable_S1_ARD_generated_by_rs_python_v1.json")
