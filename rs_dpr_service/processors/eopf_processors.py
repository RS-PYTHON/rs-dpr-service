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

from pygeoapi.process.manager.postgresql import PostgreSQLManager

from rs_dpr_service.processors.generic_processor import GenericProcessor


class S1L0Processor(GenericProcessor):
    """S1L0 Processor implementation"""

    def __init__(self, db_process_manager: PostgreSQLManager, use_mockup: bool = False):
        """
        Initialize S1L0Processor
        """
        super().__init__(
            class_name="S1L0Processor",
            env_var_id="S1L0",
            module_name="l0.s1.s1_l0_processor",
            db_process_manager=db_process_manager,
            use_mockup=use_mockup,
        )


class S3L0Processor(GenericProcessor):
    """S3L0 Processor implementation"""

    def __init__(self, db_process_manager: PostgreSQLManager, use_mockup: bool = False):
        """
        Initialize S3L0Processor
        """
        super().__init__(
            class_name="S3L0Processor",
            env_var_id="S3L0",
            module_name="l0.s3.s3_l0_processor",
            db_process_manager=db_process_manager,
            use_mockup=use_mockup,
        )


class S1ARDProcessor(GenericProcessor):
    """S1ARD Processor implementation"""

    def __init__(self, db_process_manager: PostgreSQLManager, use_mockup: bool = False):
        """
        Initialize S1ARDProcessor
        """
        super().__init__(
            class_name="S1ARDProcessor",
            env_var_id="S1ARD",
            module_name="s1_l12_rp.computing.ard_processing_units",
            db_process_manager=db_process_manager,
            use_mockup=use_mockup,
        )
