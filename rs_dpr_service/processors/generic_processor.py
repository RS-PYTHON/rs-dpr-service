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

"""S1L0 and S3L0 Processors"""
import asyncio  # for handling asynchronous tasks
import json
import os
import re
import traceback
from pathlib import Path

from dask.distributed import (  # LocalCluster,
    Client,
)
from opentelemetry import trace
from pygeoapi.process.base import BaseProcessor
from pygeoapi.process.manager.postgresql import (
    PostgreSQLManager,  # pylint: disable=C0302
)
from pygeoapi.util import JobStatus

from rs_dpr_service.dask import call_dask
from rs_dpr_service.dask.dask_cluster_handler import DaskClusterHandler
from rs_dpr_service.utils.job_logger import JobLogger
from rs_dpr_service.utils.logging import Logging
from rs_dpr_service.utils.utils import env_bool

logger = Logging.default(__name__)


# True if the 'RSPY_LOCAL_MODE' environemnt variable is set to 1, true or yes (case insensitive).
# By default: if not set or set to a different value, return False.
LOCAL_MODE: bool = env_bool("RSPY_LOCAL_MODE", default=False)


class GenericProcessor(BaseProcessor):
    """Common signature of a processor in DPR-service"""

    def __init__(
        self,
        class_name: str,
        env_var_id: str,
        module_name: str,
        db_process_manager: PostgreSQLManager,
        use_mockup: bool = False,
    ):  # pylint: disable=super-init-not-called
        self.class_name = class_name
        self.env_var_id = env_var_id
        self.module_name = module_name

        self.use_mockup = use_mockup
        self.cluster_handler = DaskClusterHandler(self._get_cluster_address(), self._get_cluster_name())
        self.job_logger = JobLogger(db_process_manager)

    def _get_cluster_address(self) -> str:
        """Returns the address of the cluster containing the processor.
        Three cases here:
            - if we use a mockup, there is a single address stored in DASK_GATEWAY__MOCKUP_ADDRESS
            - if we are in local mode with real processors, each processor has its own cluster, and the address is stored in a specific environment variable with the processor name
            - if we are in cluster mode, all the processors use the same cluster address, stored in DASK_GATEWAY__ADDRESS. The processor to use will be discrimined using its name.
        """
        if self.use_mockup:
            return os.environ["DASK_GATEWAY__MOCKUP_ADDRESS"]
        # TODO commenté pour tests
        # elif LOCAL_MODE:
        #     return os.environ[f"DASK_GATEWAY_{self.env_var_id}_ADDRESS"]
        else:
            return os.environ["DASK_GATEWAY__ADDRESS"]

    def _get_cluster_name(self) -> str:
        """Returns the name of the cluster containing the processor.
        Two cases here:
            - if we use a mockup, there is a single name stored in RSPY_DASK_DPR_SERVICE_MOCKUP_CLUSTER_NAME
            - if we use real processors, each processor has its own cluster name, and it's stored in a specific environment variable with the processor name
        """
        if self.use_mockup:
            return os.environ["RSPY_DASK_DPR_SERVICE_MOCKUP_CLUSTER_NAME"]  # "dask-eopf-mockup"
        else:
            return os.environ["RSPY_DASK_DPR_SERVICE_CLUSTER_NAME"]  # TODO A virer après tests
            return os.environ[f"RSPY_DASK_{self.env_var_id}_CLUSTER_NAME"]  # e.g. "dask-eopf"

    def replace_placeholders(self, obj):
        """
        Recursively replaces placeholders in the form ${PLACEHODER} within a nested structure (dict, list, str)
        using corresponding environment variable values.

        If an environment variable is not found, the placeholder is left unchanged and a warning is logged.

        Args:
            obj (Any): The input object, typically a dict or list, containing strings with placeholders.

        Returns:
            Any: The same structure with all placeholders replaced where possible.
        """
        pattern = re.compile(r"\$\{(\w+)\}")

        if isinstance(obj, dict):
            return {k: self.replace_placeholders(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self.replace_placeholders(item) for item in obj]
        if isinstance(obj, str):

            def replacer(match):
                key = match.group(1)
                value = os.environ.get(key)
                if value is None:
                    logger.warning("Environment variable '%s' not found; leaving placeholder unchanged.", key)
                    return match.group(0)
                return value

            return pattern.sub(replacer, obj)
        return obj

    async def get_tasktable(self):
        """Return the EOPF tasktable for a given module and class names"""
        dask_client = self.cluster_handler.setup_dask_connection()

        # Extract span infos to send to Dask
        flow_span_context = trace.get_current_span().get_span_context()

        # Manage dask tasks in a separate thread
        # starting a thread for managing the dask callbacks
        logger.debug("Starting tasks monitoring thread")
        try:
            task_table_task = dask_client.submit(
                call_dask.dpr_tasktable_task,
                caller_env=os.environ,
                flow_span_context=flow_span_context,
                use_mockup=self.use_mockup,
                module_name=self.module_name,
                class_name=self.class_name,
                pure=False,  # disable cache
            )
            res = task_table_task.result()

            # Return a default hardcoded value for the mockup
            if (not res) and self.use_mockup:
                with open(Path(__file__).parent.parent / "config" / "tasktable.json", encoding="utf-8") as tf:
                    return json.loads(tf.read())
            return res
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.exception(f"Submitting task to dask cluster failed. Reason: {e}")
            self.job_logger.log_job_execution(
                JobStatus.failed,
                None,
                f"Submitting task to dask cluster failed. Reason: {e}",
            )
            return {}

    # Override from BaseProcessor, execute is async in RSPYProcessor
    async def execute(  # pylint: disable=too-many-return-statements, invalid-overridden-method
        self,
        data: dict,
        outputs=None,
    ) -> tuple[str, dict]:
        """
        Asynchronously execute the dpr process in the dask cluster
        """

        # self.logger.debug(f"Executing staging processor for {data}")

        self.job_logger.log_job_execution(JobStatus.running, 0, "Processor execution started")
        # Start execution
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If the loop is running, schedule the async function
            asyncio.create_task(self.start_processor(data))
        else:
            # If the loop is not running, run it until complete
            loop.run_until_complete(self.start_processor(data))

        return self.job_logger.get_execute_result()

    async def start_processor(  # pylint: disable=too-many-return-statements
        self,
        data: dict,
    ) -> tuple[str, dict]:
        """
        Method used to trigger dask distributed streaming process.
        It creates dask client object, gets the external dpr_payload sources access token
        Prepares the tasks for execution
        Manage eventual runtime exceptions

        Args:
            catalog_collection (str): Name of the catalog collection.

        Returns:
            tuple: tuple of MIME type and process response (dictionary containing the job ID and a
                status message).
                Example: ("application/json", {"running": <job_id>})
        """
        logger.debug("Starting main loop")

        try:
            dask_client = self.cluster_handler.setup_dask_connection()
        except KeyError as ke:
            logger.error(f"Failed to start the dpr-service process: No env var {ke} found")
            return self.job_logger.log_job_execution(JobStatus.failed, 0, str(ke))
        except RuntimeError as runtime_error:
            logger.error("Failed to start the dpr-service process")
            return self.job_logger.log_job_execution(JobStatus.failed, 0, str(runtime_error))

        self.job_logger.log_job_execution(JobStatus.running, 0, "Sending task to the dask cluster")

        # Manage dask tasks in a separate thread
        # starting a thread for managing the dask callbacks
        logger.debug("Starting tasks monitoring thread")
        try:
            await asyncio.to_thread(
                self.manage_dask_tasks,
                dask_client,
                data,
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.job_logger.log_job_execution(JobStatus.failed, 0, f"Error from tasks monitoring thread: {e}")

        # cleanup by disconnecting the dask client
        dask_client.close()

        return self.job_logger.get_execute_result()

    def manage_dask_tasks(self, client: Client, data: dict):
        """
        Manages Dask tasks where the dpr processor is started.
        """
        logger.info("Tasks monitoring started")
        if not client:
            logger.error("The dask cluster client object is not created. Exiting")
            self.job_logger.log_job_execution(
                JobStatus.failed,
                None,
                "Submitting task to dask cluster failed. Dask cluster client object is not created",
            )
            return

        self.job_logger.log_job_execution(
            JobStatus.running,
            50,
            "In progress",
        )
        try:
            # For the mockup, replace placeholders by env vars.
            # For the real processor, it is done automatically by eopf.
            if self.use_mockup:
                data = self.replace_placeholders(data)

            # Run processor in the dask client
            dpr_task = client.submit(
                call_dask.dpr_processor_task,
                caller_env=os.environ,
                data=data,
                use_mockup=self.use_mockup,
                pure=False,  # disable cache
            )

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.exception(f"Submitting task to dask cluster failed. Reason: {e}")
            self.job_logger.log_job_execution(
                JobStatus.failed,
                None,
                f"Submitting task to dask cluster failed. Reason: {e}",
            )
            return

        try:
            res = dpr_task.result()  # This will raise the exception from the task if it failed
            logger.info("%s Task streaming completed", dpr_task.key)

        except Exception as task_e:  # pylint: disable=broad-exception-caught
            logger.error("Task failed with exception: %s", traceback.format_exc())
            # Update status for the job
            self.job_logger.log_job_execution(JobStatus.failed, None, f"The dpr processing task failed: {task_e}")
            return

        # Update status and insert the result of the dask task in the jobs table
        self.job_logger.log_job_execution(JobStatus.successful, 100, str(res))
        # write the results in a s3 bucket file

        # Update the subscribers for token refreshment
        logger.info("Tasks monitoring finished")
