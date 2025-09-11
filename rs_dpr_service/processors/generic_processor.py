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
from rs_dpr_service.utils.settings import LOCAL_MODE, ExperimentalConfig

logger = Logging.default(__name__)


class GenericProcessor(BaseProcessor):
    """
    Common signature of a processor in DPR-service.

    NOTE: a new instance of this class is called for every endpoint call.
    """

    def __init__(
        self,
        db_process_manager: PostgreSQLManager,
        cluster_name: str = "",
        local_mode_address: str = "",
        tasktable_module: str = "",
        tasktable_class: str = "",
    ):  # pylint: disable=super-init-not-called
        self.use_mockup = False
        self.tasktable_module = tasktable_module
        self.tasktable_class = tasktable_class
        self.job_logger = JobLogger(db_process_manager)
        self.cluster_handler = DaskClusterHandler(cluster_name, local_mode_address)

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

        try:
            # Extract span infos to send to Dask
            span_context = trace.get_current_span().get_span_context()

            dpr_processor = call_dask.ProcessorCaller(
                caller_env=dict(os.environ),
                span_context=span_context,
                cluster_address=self.cluster_handler.cluster_address,
                cluster_instance=self.cluster_handler.cluster_instance,
                data={},  # not used for the tasktables
                use_mockup=self.use_mockup,
            )

            # Run processor in the dask client
            task_table_task = dask_client.submit(
                dpr_processor.get_tasktable,
                module_name=self.tasktable_module,
                class_name=self.tasktable_class,
                pure=False,  # disable cache
            )
            res = task_table_task.result()

            # Return a default hardcoded value for the mockup
            if (not res) and self.use_mockup:
                with open(Path(__file__).parent.parent / "config" / "tasktable.json", encoding="utf-8") as tf:
                    return json.loads(tf.read())
            return res
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.exception(f"Submitting task to dask cluster failed. Reason: {traceback.format_exc()}")
            raise e
        finally:
            # cleanup by disconnecting the dask client
            dask_client.close()

    # Override from BaseProcessor, execute is async in RSPYProcessor
    async def execute(  # pylint: disable=invalid-overridden-method
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

    async def start_processor(
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
            experimental_config = ExperimentalConfig(**data.get("experimental_config", {}))

            # For testing: run the eopf-cpm scheduler on local.
            # It will then init a dask LocalCluster instance itself.
            if LOCAL_MODE and experimental_config.local_cluster.enabled:
                dask_client = None

            # Nominal case: run the eopf-cpm scheduler on a dedicated cluster pod, not locally.
            else:
                dask_client = self.cluster_handler.setup_dask_connection()
        except KeyError as ke:
            return self.job_logger.log_job_execution(
                JobStatus.failed,
                0,
                f"Failed to start the dpr-service process: No env var {ke} found",
                log_exception=True,
            )
        except RuntimeError:
            return self.job_logger.log_job_execution(
                JobStatus.failed,
                0,
                f"Failed to start the dpr-service process: {traceback.format_exc()}",
                log_exception=True,
            )

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
        except Exception:  # pylint: disable=broad-exception-caught
            self.job_logger.log_job_execution(
                JobStatus.failed,
                0,
                f"Error from tasks monitoring thread: {traceback.format_exc()}",
                log_exception=True,
            )

        # cleanup by disconnecting the dask client
        if dask_client:
            dask_client.close()

        return self.job_logger.get_execute_result()

    def manage_dask_tasks(self, dask_client: Client | None, data: dict):
        """
        Manages Dask tasks where the dpr processor is started.
        """
        logger.info("Tasks monitoring started")
        res = None

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

            # Extract span infos to send to Dask
            span_context = trace.get_current_span().get_span_context()

            dpr_processor = call_dask.ProcessorCaller(
                caller_env=dict(os.environ),
                span_context=span_context,
                cluster_address=self.cluster_handler.cluster_address,
                cluster_instance=self.cluster_handler.cluster_instance,
                data=data,
                use_mockup=self.use_mockup,
            )

            # Nominal usecase: run processor in the dask client
            if dask_client:
                dpr_task = dask_client.submit(
                    dpr_processor.run_processor,
                    pure=False,  # disable cache
                )

            # Specific case for local debugging
            else:
                dpr_task = None
                res = dpr_processor.run_processor()

        except Exception:  # pylint: disable=broad-exception-caught
            if not dask_client:
                logger.exception(traceback.format_exc())
                raise
            self.job_logger.log_job_execution(
                JobStatus.failed,
                None,
                f"Submitting task to dask cluster failed. Reason: {traceback.format_exc()}",
                log_exception=True,
            )
            return

        try:
            if dpr_task:
                res = dpr_task.result()  # This will raise the exception from the task if it failed
                logger.info("%s Task streaming completed", dpr_task.key)

        except Exception:  # pylint: disable=broad-exception-caught
            # Update status for the job
            self.job_logger.log_job_execution(
                JobStatus.failed,
                None,
                f"The dpr processing task failed: {traceback.format_exc()}",
                log_exception=True,
            )
            return

        # Update status and insert the result of the dask task in the jobs table
        self.job_logger.log_job_execution(JobStatus.successful, 100, str(res))
        # write the results in a s3 bucket file

        # Update the subscribers for token refreshment
        logger.info("Tasks monitoring finished")
