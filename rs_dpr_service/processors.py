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
import uuid
from datetime import datetime
from pathlib import Path

from dask.distributed import (  # LocalCluster,
    Client,
)
from dask_gateway import Gateway
from dask_gateway.auth import BasicAuth, JupyterHubAuth
from opentelemetry import trace
from pygeoapi.process.base import BaseProcessor
from pygeoapi.process.manager.postgresql import (
    PostgreSQLManager,  # pylint: disable=C0302
)
from pygeoapi.util import JobStatus

from rs_dpr_service import call_dask
from rs_dpr_service.utils.logging import Logging
from rs_dpr_service.utils.utils import env_bool

default_logger = Logging.default(__name__)
logger = Logging.default(__name__)


# True if the 'RSPY_LOCAL_MODE' environemnt variable is set to 1, true or yes (case insensitive).
# By default: if not set or set to a different value, return False.
LOCAL_MODE: bool = env_bool("RSPY_LOCAL_MODE", default=False)

# Cluster mode is the opposite of local mode
CLUSTER_MODE: bool = not LOCAL_MODE


class DaskClusterHandler:
    def __init__(self, cluster_address: str, cluster_name: str):
        self.cluster_name = cluster_name
        self.cluster_address = cluster_address
        self.cluster = None

    def dask_cluster_connect(self):  # pylint: disable=too-many-branches, too-many-statements, too-many-locals
        """Connects a dask cluster scheduler
        Establishes a connection to a Dask cluster, either in a local environment or via a Dask Gateway in
        a Kubernetes cluster. This method checks if the cluster is already created (for local mode) or connects
        to a Dask Gateway to find or create a cluster scheduler (for Kubernetes mode, see RSPY_LOCAL_MODE env var).

        1. **Local Mode**:
        - If `self.cluster` already exists, it assumes the Dask cluster was created when the application started,
            and proceeds without creating a new cluster.

        2. **Kubernetes Mode**:
        - If `self.cluster` is not already defined, the method attempts to connect to a Dask Gateway
            (using environment variables `DASK_GATEWAY__ADDRESS` and `DASK_GATEWAY__AUTH__TYPE`) to
            retrieve a list of existing clusters.
        - If no clusters are available, it attempts to create a new cluster scheduler.

        Raises:
            RuntimeError: Raised if the cluster name is None, required environment variables are missing,
                        cluster creation fails or authentication errors occur.
            KeyError: Raised if the necessary Dask Gateway environment variables (`DASK_GATEWAY__ADDRESS`,
                `DASK_GATEWAY__AUTH__TYPE`, `RSPY_DASK_DPR_SERVICE_CLUSTER_NAME`, `JUPYTERHUB_API_TOKEN` ) are not set.
            IndexError: Raised if no clusters are found in the Dask Gateway and new cluster creation is attempted.
            dask_gateway.exceptions.GatewayServerError: Raised when there is a server-side error in Dask Gateway.
            dask_gateway.exceptions.AuthenticationError: Raised if authentication to the Dask Gateway fails.
            dask_gateway.exceptions.ClusterLimitExceeded: Raised if the limit on the number of clusters is exceeded.

        Behavior:
        1. **Cluster Creation and Connection**:
            - In Kubernetes mode, the method tries to connect to an existing cluster or creates
            a new one if none exists.
            - Error handling includes catching issues like missing environment variables, authentication failures,
            cluster creation timeouts, or exceeding cluster limits.

        2. **Logging**:
            - Logs the list of available clusters if connected via the Dask Gateway.
            - Logs the success of the connection or any errors encountered during the process.
            - Logs the Dask dashboard URL and the number of active workers.

        3. **Client Initialization**:
            - Once connected to the Dask cluster, the method creates a Dask `Client` object for managing tasks
            and logs the number of running workers.
            - If no workers are found, it scales the cluster to 1 worker.

        4. **Error Handling**:
            - Handles various exceptions during the connection and creation process, including:
            - Missing environment variables.
            - Failures during cluster creation.
            - Issues related to cluster scaling, worker retrieval, or client creation.
            - If an error occurs, the method logs the error and attempts to gracefully handle failure.

        Returns:
            Dask client
        """

        # If self.cluster is already initialized, it means the application is running in local mode, and
        # the cluster was created when the application started.

        # Connect to the gateway and get the list of the clusters
        try:
            # In local mode, authenticate to the dask cluster with username/password
            if LOCAL_MODE:
                gateway_auth = BasicAuth(
                    os.environ["LOCAL_DASK_USERNAME"],
                    os.environ["LOCAL_DASK_PASSWORD"],
                )

            # Cluster mode
            else:
                # check the auth type, only jupyterhub type supported for now
                auth_type = os.environ["DASK_GATEWAY__AUTH__TYPE"]
                # Handle JupyterHub authentication
                if auth_type == "jupyterhub":
                    gateway_auth = JupyterHubAuth(api_token=os.environ["JUPYTERHUB_API_TOKEN"])
                else:
                    logger.error(f"Unsupported authentication type: {auth_type}")
                    raise RuntimeError(f"Unsupported authentication type: {auth_type}")

            gateway = Gateway(
                address=self.cluster_address,
                auth=gateway_auth,
            )

            # Sort the clusters by newest first
            clusters = sorted(gateway.list_clusters(), key=lambda cluster: cluster.start_time, reverse=True)
            logger.debug(f"Cluster list for gateway {self.cluster_address!r}: {clusters}")

            # In local mode, get the first cluster from the gateway.
            cluster_id = None
            if LOCAL_MODE:
                if clusters:
                    cluster_id = clusters[0].name

            # In cluster mode, get the identifier of the cluster whose name is equal to the cluster_name variable.
            # Protection for the case when this cluster does not exit
            else:
                logger.info(f"my cluster name: {self.cluster_name}")

                for cluster in clusters:
                    logger.info(f"Existing cluster names: {cluster.options.get('cluster_name')}")

                    is_equal = cluster.options.get("cluster_name") == self.cluster_name
                    logger.info(f"Is equal: {is_equal}")

                cluster_id = next(
                    (
                        cluster.name
                        for cluster in clusters
                        if isinstance(cluster.options, dict)
                        and cluster.options.get("cluster_name") == self.cluster_name
                    ),
                    None,
                )
                logger.info(f"Cluster id: {cluster_id}")

            if not cluster_id:
                raise IndexError(f"Dask cluster with 'cluster_name'={self.cluster_name!r} was not found.")

            self.cluster = gateway.connect(cluster_id)
            if not self.cluster:
                logger.exception("Failed to create the cluster")
                raise RuntimeError("Failed to create the cluster")
            logger.info(f"Successfully connected to the {self.cluster_name} dask cluster")

            # This cluster id is needed by the eopf dask scheduler to connect later to this cluster.
            # This is something like "dask-gateway.17e196069443463495547eb97f532834"
            os.environ["DASK_CLUSTER_EOPF_NAME"] = cluster_id

        except KeyError as e:
            logger.exception(
                "Failed to retrieve the required connection details for "
                "the Dask Gateway from one or more of the following environment variables: "
                "DASK_GATEWAY__ADDRESS, RSPY_DASK_DPR_SERVICE_CLUSTER_NAME, "
                f"JUPYTERHUB_API_TOKEN, DASK_GATEWAY__AUTH__TYPE. {e}",
            )

            raise RuntimeError(
                f"Failed to retrieve the required connection details for Dask Gateway. Missing key:{e}",
            ) from e
        except IndexError as e:
            logger.exception(f"Failed to find the specified dask cluster: {e}")
            raise RuntimeError(f"No dask cluster named '{self.cluster_name}' was found.") from e

        logger.debug("Cluster dashboard: %s", self.cluster.dashboard_link)
        # create the client as well
        client = Client(self.cluster)

        # Forward logging from dask workers to the caller
        client.forward_logging()

        # Upload local module to the dask client.
        call_dask.upload_this_module(client)

        def set_dask_env(host_env: dict):
            """Pass environment variables to the dask workers."""
            for name in ["S3_ACCESSKEY", "S3_SECRETKEY", "S3_ENDPOINT", "S3_REGION"]:
                os.environ[name] = host_env[name]

            # Some kind of workaround for boto3 to avoid checksum being added inside
            # the file contents uploaded to the s3 bucket e.g. x-amz-checksum-crc32:xxx
            # See: https://github.com/boto/boto3/issues/4435
            os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "when_required"
            os.environ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "when_required"

        client.run(set_dask_env, os.environ)

        # This is a temporary fix for the dask cluster settings which does not create a scheduler by default
        # This code should be removed as soon as this is fixed in the kubernetes cluster
        try:
            logger.debug(f"{client.get_versions(check=True)}")
            workers = client.scheduler_info()["workers"]
            logger.info(f"Number of running workers: {len(workers)}")

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.exception(f"Dask cluster client failed: {e}")
            raise RuntimeError(f"Dask cluster client failed: {e}") from e
        if len(workers) == 0:
            logger.info("No workers are currently running in the Dask cluster. Scaling up to 1.")
            self.cluster.scale(1)

        # Check the cluster dashboard
        logger.debug(f"Dask Client: {client} | Cluster dashboard: {self.cluster.dashboard_link}")

        return client


class JobLogger:
    def __init__(self, db_process_manager: PostgreSQLManager):
        self.job_id: str = str(uuid.uuid4())  # Generate a unique job ID
        self.message: str = "Processing Unit was created"
        self.progress: float = 0.0
        self.status = JobStatus.accepted
        self.db_process_manager = db_process_manager
        self.create_job_execution()

    def get_execute_result(self) -> tuple[str, dict]:
        return "application/json", {self.status.value: self.job_id}

    def create_job_execution(self):
        """
        Creates a new job execution entry and tracks its status.

        This method creates a job entry in the tracker with the current job's ID, status,
        progress, and message. The job information is stored in a persistent tracker to allow
        monitoring and updating of the job's execution state.

        The following information is stored:
            - `job_id`: The unique identifier for the job.
            - `status`: The current status of the job, converted to a JSON-serializable format.
            - `progress`: The progress of the job execution.
            - `message`: Additional details about the job's execution.

        Notes:
            - The `self.tracker` is expected to have an `insert` method to store the job information.
            - The status is converted to JSON using `JobStatus.to_json()`.

        """
        job_metadata = {
            "identifier": self.job_id,
            "processID": "dpr-service",
            "status": self.status.value,
            "progress": self.progress,
            "message": self.message,
        }
        self.db_process_manager.add_job(job_metadata)

    def log_job_execution(
        self,
        status: JobStatus | None = None,
        progress: int | None = None,
        message: str | None = None,
    ) -> tuple[str, dict]:
        """
        Method used to log progress into db.

        Args:
            status (JobStatus): new job status
            progress (int): new job progress (percentage)
            message (str): new job current information message

        Returns:
            tuple: tuple of MIME type and process response (dictionary containing the job ID and a
                status message).
                Example: ("application/json", {"running": <job_id>})
        """
        # Update both runtime and db status and progress

        self.status = status if status else self.status
        self.progress = progress if progress else self.progress
        self.message = message if message else self.message

        update_data = {
            "status": self.status.value,
            "progress": self.progress,
            "message": self.message,
            "updated": datetime.now(),  # Update updated each time a change is made
        }
        if status == JobStatus.failed:
            logger.error(f"Updating failed job {self.job_id}: {update_data}")
        else:
            logger.info(f"Updating job {self.job_id}: {update_data}")

        self.db_process_manager.update_job(self.job_id, update_data)
        return self.get_execute_result()


class GenericProcessor(BaseProcessor):
    def __init__(
        self,
        class_name: str,
        env_var_id: str,
        module_name: str,
        db_process_manager: PostgreSQLManager,
        use_mockup: bool = False,
    ):
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
        dask_client = self.cluster_handler.dask_cluster_connect()

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
            dask_client = self.cluster_handler.dask_cluster_connect()
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
