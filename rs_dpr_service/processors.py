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
import ast
import asyncio  # for handling asynchronous tasks
import json
import logging
import os
import os.path as osp
import re
import subprocess
import time
import uuid
from datetime import datetime
from pathlib import Path

import yaml
from dask.distributed import (  # LocalCluster,
    Client,
)
from dask_gateway import Gateway
from dask_gateway.auth import BasicAuth, JupyterHubAuth
from pygeoapi.process.base import BaseProcessor
from pygeoapi.process.manager.postgresql import (
    PostgreSQLManager,  # pylint: disable=C0302
)
from pygeoapi.util import JobStatus
from starlette.datastructures import Headers
from starlette.requests import Request

logger = logging.getLogger("processors")
logger.setLevel(logging.DEBUG)


def env_bool(var: str, default: bool) -> bool:
    """
    Return True if an environemnt variable is set to 1, true or yes (case insensitive).
    Return False if set to 0, false or no (case insensitive).
    Return the default value if not set or set to a different value.
    """
    val = os.getenv(var, str(default)).lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return True
    if val in ("n", "no", "f", "false", "off", "0"):
        return False
    return default


# True if the 'RSPY_LOCAL_MODE' environemnt variable is set to 1, true or yes (case insensitive).
# By default: if not set or set to a different value, return False.
LOCAL_MODE: bool = env_bool("RSPY_LOCAL_MODE", default=False)

# Cluster mode is the opposite of local mode
CLUSTER_MODE: bool = not LOCAL_MODE


def dpr_processor_task(  # pylint: disable=R0914, R0917
    dpr_payload: dict,
):
    """
    Dpr processing inside the dask cluster
    """

    logger_dask = logging.getLogger(__name__)
    print("Dask task running - print() test")
    logger_dask.info("The dpr processing task started")
    logger_dask.info("Task started. Received dpr_payload = %s", json.dumps(dpr_payload, indent=2))
    use_mockup = dpr_payload.get("use_mockup", False)
    try:
        payload_abs_path = osp.join("/", os.getcwd(), "payload.cfg")
        with open(payload_abs_path, "w+", encoding="utf-8") as payload:
            payload.write(yaml.safe_dump(dpr_payload))
    except Exception as e:
        logger_dask.exception("Exception during payload file creation: %s", e)
        raise

    command = ["eopf", "trigger", "local", payload_abs_path]
    wd = "."
    if use_mockup:
        command = ["python3.11", "DPR_processor_mock.py", "-p", payload_abs_path]
        wd = "/src/DPR"
    logger.debug(f"Working directory for subprocess: {wd} (type: {type(wd)})")
    # Trigger EOPF processing, catch output
    assert isinstance(wd, str), f"Expected working directory (cwd) to be str, got {type(wd)}"
    with subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=wd,
    ) as p:
        assert p.stdout is not None  # For mypy
        # Log contents
        log_str = ""
        return_response = {}
        # Write output to a log file and string + redirect to the prefect logger
        with open(Path(payload_abs_path).with_suffix(".log").name, "w+", encoding="utf-8") as log_file:
            while (line := p.stdout.readline()) != "":

                # The log prints password in clear e.g 'key': '<my-secret>'... hide them with a regex
                for key in (
                    "key",
                    "secret",
                    "endpoint_url",
                    "region_name",
                    "api_token",
                    "password",
                ):
                    line = re.sub(rf"(\W{key}\W)[^,}}]*", r"\1: ***", line)

                # Write to log file and string
                log_file.write(line)
                log_str += line

                # Write to prefect logger if not empty
                line = line.rstrip()
                if line:
                    logger.info(line)

            logger.info(f"log_str = {log_str}")
            # search for the JSON-like part, parse it, and ignore the rest.
            match = re.search(r"(\[\s*\{.*\}\s*\])", log_str, re.DOTALL)
            if not match:
                raise ValueError("No valid dpr_payload structure found in the output.")

            payload_str = match.group(1)

            # Use `ast.literal_eval` to safely evaluate the structure
            try:
                # payload_str is a string that looks like a JSON, extracted from the dpr mockup's raw output.
                # ast.literal_eval() parses that string and returns the actual Python object (not just the string).
                return_response = ast.literal_eval(payload_str)
            except Exception as e:
                raise ValueError(f"Failed to parse dpr_payload structure: {e}") from e

        try:
            # Wait for the execution to finish
            status_code = p.wait()

            # Raise exception if the status code is != 0
            if status_code:
                raise Exception("EOPF error, please see the log.")  # pylint: disable=broad-exception-raised

        # In all cases, upload the reports dir to the s3 bucket.
        finally:
            time.sleep(1)

        return return_response


def dpr_tasktable_task(use_mockup=False):
    """
    Dpr tasktable inside the dask cluster
    """

    logger_dask = logging.getLogger(__name__)
    logger_dask.info("The dpr triggering tasktable task started")

    command = ["eopf", "trigger", "tasktable"]
    if use_mockup:
        command = ["sleep", "1"]
    # Trigger EOPF tasktable command
    tasktable_result: dict = {}
    with subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ) as p:
        # This will be activated when trigger tasktable cmd will work
        # assert p.stdout is not None  # For mypy
        # when the eopf command will work, delete the following print and handle the p.stdout
        print(p.stdout)
        # tasktable_result = p.stdout

    return tasktable_result


class GeneralProcessor(BaseProcessor):
    """Common signature of a processor in DPR-service"""

    def __init__(
        self,
        credentials: Request,
        db_process_manager: PostgreSQLManager,
        name,
    ):  # pylint: disable=super-init-not-called
        """
        Initialize the general processor
        """
        #################
        # Locals
        self.name = name
        self.logger = logger
        self.request = credentials
        self.headers: Headers = credentials.headers

        # Database section
        self.job_id: str = str(uuid.uuid4())  # Generate a unique job ID
        self.message: str = "Processing Unit was created"
        self.progress: float = 0.0
        self.db_process_manager = db_process_manager
        self.status = JobStatus.accepted
        self.create_job_execution()
        #################
        # Inputs section
        self.assets_info: list = []

        self.cluster = None
        # self.catalog_bucket = os.environ.get("RSPY_CATALOG_BUCKET", "rs-cluster-catalog")

    async def get_tasktable(self, data, client=None, name=None):  # pylint: disable=W0613
        """Will execute eopf tasktable command when available"""
        # Disabled;
        # if name:
        #     with subprocess.Popen(
        #         ["eopf", "trigger", "tasktable", name],
        #         stdout=subprocess.PIPE,
        #         stderr=subprocess.STDOUT,
        #         text=True,
        #     ) as p:
        #         return p.stdout()
        use_mockup = False
        if data and isinstance(data, dict):
            use_mockup = data.get("use_mockup", False)
            if use_mockup:
                os.environ["DASK_CLUSTER_EOPF_NAME"] = os.environ["RSPY_DASK_DPR_SERVICE_MOCKUP_CLUSTER_NAME"]
                os.environ["DASK_GATEWAY_EOPF_ADDRESS"] = os.environ["DASK_GATEWAY__MOCKUP_ADDRESS"]
        with open(Path(__file__).parent.parent / "config" / "tasktable.json", encoding="utf-8") as tf:
            tasktable_data = json.loads(tf.read())

        # the need for the usage of env vars instead of simply set input params for dask_cluster_connect is because
        # the payload.cfg from the user is comming with a `dask_context:` section that contains
        # values to be replaced such as:
        # cluster_config:
        #   address: ${DASK_GATEWAY_EOPF_ADDRESS}
        # .....
        # reuse_cluster: ${DASK_CLUSTER_EOPF_NAME}
        dask_client = self.dask_cluster_connect(
            os.environ["DASK_CLUSTER_EOPF_NAME"],
            os.environ["DASK_GATEWAY_EOPF_ADDRESS"],
        )

        # Manage dask tasks in a separate thread
        # starting a thread for managing the dask callbacks
        self.logger.debug("Starting tasks monitoring thread")
        try:
            task_table_task = dask_client.submit(dpr_tasktable_task, use_mockup)
            res = task_table_task.result()
            # the eopf tasktable is supposed to fail at the current time, so return something hardcoded
            if not res:
                res = tasktable_data
            return res
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.logger.exception(f"Submitting task to dask cluster failed. Reason: {e}")
            self.log_job_execution(JobStatus.failed, None, f"Submitting task to dask cluster failed. Reason: {e}")
            return {}

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

    def manage_dask_tasks(self, client: Client, dpr_payload: dict):
        """
        Manages Dask tasks where the dpr processor is started.


        """
        self.logger.info("Tasks monitoring started")
        if not client:
            self.logger.error("The dask cluster client object is not created. Exiting")
            self.log_job_execution(
                JobStatus.failed,
                None,
                "Submitting task to dask cluster failed. Dask cluster client object is not created",
            )
            return

        self.log_job_execution(
            JobStatus.running,
            50,
            "In progress",
        )
        try:
            dpr_task = client.submit(dpr_processor_task, self.replace_placeholders(dpr_payload))
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.logger.exception(f"Submitting task to dask cluster failed. Reason: {e}")
            self.log_job_execution(JobStatus.failed, None, f"Submitting task to dask cluster failed. Reason: {e}")
            return
        # counter to be used for percentage

        try:
            res = dpr_task.result()  # This will raise the exception from the task if it failed
            self.logger.info("%s Task streaming completed", dpr_task.key)

        except Exception as task_e:  # pylint: disable=broad-exception-caught
            self.logger.error("Task failed with exception: %s", task_e)
            # Update status for the job
            self.log_job_execution(JobStatus.failed, None, f"The dpr processing task failed: {task_e}")
            return

        # Update status and insert the result of the dask task in the jobs table
        self.log_job_execution(JobStatus.successful, 100, str(res))
        # write the results in a s3 bucket file

        # Update the subscribers for token refreshment
        self.logger.info("Tasks monitoring finished")

    def dask_cluster_connect(
        self,
        cluster_name,
        cluster_address,
    ):  # pylint: disable=too-many-branches, too-many-statements, too-many-locals
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
                    self.logger.error(f"Unsupported authentication type: {auth_type}")
                    raise RuntimeError(f"Unsupported authentication type: {auth_type}")

            gateway = Gateway(
                address=cluster_address,
                auth=gateway_auth,
            )

            # Sort the clusters by newest first
            clusters = sorted(gateway.list_clusters(), key=lambda cluster: cluster.start_time, reverse=True)
            self.logger.debug(f"Cluster list for gateway {cluster_address!r}: {clusters}")

            # In local mode, get the first cluster from the gateway.
            cluster_id = None
            if LOCAL_MODE:
                if clusters:
                    cluster_id = clusters[0].name

            # In cluster mode, get the identifier of the cluster whose name is equal to the cluster_name variable.
            # Protection for the case when this cluster does not exit
            else:
                self.logger.info(f"my cluster name: {cluster_name}")

                for cluster in clusters:
                    self.logger.info(f"Existing cluster names: {cluster.options.get('cluster_name')}")

                    is_equal = cluster.options.get("cluster_name") == cluster_name
                    self.logger.info(f"Is equal: {is_equal}")

                cluster_id = next(
                    (
                        cluster.name
                        for cluster in clusters
                        if isinstance(cluster.options, dict) and cluster.options.get("cluster_name") == cluster_name
                    ),
                    None,
                )
                self.logger.info(f"Cluster id vaut: {cluster_id}")

            if not cluster_id:
                raise IndexError(f"Dask cluster with 'cluster_name'={cluster_name!r} was not found.")

            self.cluster = gateway.connect(cluster_id)
            if not self.cluster:
                self.logger.exception("Failed to create the cluster")
                raise RuntimeError("Failed to create the cluster")
            self.logger.info(f"Successfully connected to the {cluster_name} dask cluster")

        except KeyError as e:
            self.logger.exception(
                "Failed to retrieve the required connection details for "
                "the Dask Gateway from one or more of the following environment variables: "
                "DASK_GATEWAY__ADDRESS, RSPY_DASK_DPR_SERVICE_CLUSTER_NAME, "
                f"JUPYTERHUB_API_TOKEN, DASK_GATEWAY__AUTH__TYPE. {e}",
            )

            raise RuntimeError(
                f"Failed to retrieve the required connection details for Dask Gateway. Missing key:{e}",
            ) from e
        except IndexError as e:
            self.logger.exception(f"Failed to find the specified dask cluster: {e}")
            raise RuntimeError(f"No dask cluster named '{cluster_name}' was found.") from e

        self.logger.debug("Cluster dashboard: %s", self.cluster.dashboard_link)
        # create the client as well
        client = Client(self.cluster)

        # Forward logging from dask workers to the caller
        client.forward_logging()

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
            self.logger.debug(f"{client.get_versions(check=True)}")
            workers = client.scheduler_info()["workers"]
            self.logger.info(f"Number of running workers: {len(workers)}")

        except Exception as e:  # pylint: disable=broad-exception-caught
            self.logger.exception(f"Dask cluster client failed: {e}")
            raise RuntimeError(f"Dask cluster client failed: {e}") from e
        if len(workers) == 0:
            self.logger.info("No workers are currently running in the Dask cluster. Scaling up to 1.")
            self.cluster.scale(1)
        # end of TODO

        # Check the cluster dashboard
        self.logger.debug(f"Dask Client: {client} | Cluster dashboard: {self.cluster.dashboard_link}")

        return client

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

        self.log_job_execution(JobStatus.running, 0, "Successfully searched catalog")
        # Start execution
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If the loop is running, schedule the async function
            asyncio.create_task(self.start_processor(data))
        else:
            # If the loop is not running, run it until complete
            loop.run_until_complete(self.start_processor(data))

        return self._get_execute_result()

    async def start_processor(  # pylint: disable=too-many-return-statements
        self,
        dpr_payload: dict,
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
        self.logger.debug("Starting main loop")

        try:
            if dpr_payload.get("use_mockup", False):
                os.environ["DASK_CLUSTER_EOPF_NAME"] = os.environ["RSPY_DASK_DPR_SERVICE_MOCKUP_CLUSTER_NAME"]
                os.environ["DASK_GATEWAY_EOPF_ADDRESS"] = os.environ["DASK_GATEWAY__MOCKUP_ADDRESS"]
            # the need for the usage of env vars instead of simply set input params for dask_cluster_connect is because
            # the payload.cfg from the user is comming with a `dask_context:` section that contains
            # values to be replaced such as:
            # cluster_config:
            #   address: ${DASK_GATEWAY_EOPF_ADDRESS}
            # .....
            # reuse_cluster: ${DASK_CLUSTER_EOPF_NAME}

            dask_client = self.dask_cluster_connect(
                os.environ["DASK_CLUSTER_EOPF_NAME"],
                os.environ["DASK_GATEWAY_EOPF_ADDRESS"],
            )
        except KeyError as ke:
            self.logger.error(f"Failed to start the dpr-service process: No env var {ke} found")
            return self.log_job_execution(JobStatus.failed, 0, str(ke))
        except RuntimeError as runtime_error:
            self.logger.error("Failed to start the dpr-service process")
            return self.log_job_execution(JobStatus.failed, 0, str(runtime_error))

        self.log_job_execution(JobStatus.running, 0, "Sending task to the dask cluster")

        # Manage dask tasks in a separate thread
        # starting a thread for managing the dask callbacks
        self.logger.debug("Starting tasks monitoring thread")
        try:
            await asyncio.to_thread(
                self.manage_dask_tasks,
                dask_client,
                dpr_payload,
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.log_job_execution(JobStatus.failed, 0, f"Error from tasks monitoring thread: {e}")

        # cleanup by disconnecting the dask client
        self.assets_info = []
        dask_client.close()

        return self._get_execute_result()

    def _get_execute_result(self) -> tuple[str, dict]:
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
            self.logger.error(f"Updating failed job {self.job_id}: {update_data}")
        else:
            self.logger.info(f"Updating job {self.job_id}: {update_data}")

        self.db_process_manager.update_job(self.job_id, update_data)
        return self._get_execute_result()

    def wait_for_dask_completion(self, client: Client):
        """Waits for all Dask tasks to finish before proceeding."""
        timeout = int(os.environ.get("RSPY_STAGING_TIMEOUT", 600))
        while timeout > 0:
            if not client.call_stack():
                break  # No tasks running anymore
            time.sleep(1)
            timeout -= 1


class S1L0Processor(GeneralProcessor):
    """S1L0 Processor implementation"""

    def __init__(
        self,
        credentials: Request,
        db_process_manager: PostgreSQLManager,
        # cluster: LocalCluster,
    ):  # pylint: disable=super-init-not-called
        """
        Initialize S1L0Processor
        """
        super().__init__(credentials, db_process_manager, "S1L0Processor")

    # Will be activated later
    # def get_tasktable(self, name="l0.s1.s1_l0_processor S1L0Processor"):
    #     return super().get_tasktable(name)


class S3L0Processor(GeneralProcessor):
    """S3L0 Processor implementation"""

    def __init__(
        self,
        credentials: Request,
        db_process_manager: PostgreSQLManager,
        # cluster: LocalCluster,
    ):  # pylint: disable=super-init-not-called
        """
        Initialize S1L0Processor
        """
        super().__init__(credentials, db_process_manager, "S3L0Processor")

    # Will be activated later
    # def get_tasktable(self, name="l0.s3.s3_l0_processor S3L0Processor"):
    #     return super().get_tasktable(name)


# Register the processor

processors = {"S1L0_processor": S1L0Processor, "S3L0_processor": S3L0Processor}
