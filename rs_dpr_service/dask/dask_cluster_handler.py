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

"""Module to handle connection to Dask cluster."""

import os

from dask.distributed import (  # LocalCluster,
    Client,
)
from dask_gateway import Gateway, GatewayCluster
from dask_gateway.auth import BasicAuth, JupyterHubAuth

from rs_dpr_service.dask import call_dask
from rs_dpr_service.utils.logging import Logging
from rs_dpr_service.utils.settings import LOCAL_MODE, set_dask_env

logger = Logging.default(__name__)


class DaskClusterHandler:  # pylint: disable=too-few-public-methods
    """
    Class to handle connection to Dask cluster.

    NOTE: a new instance of this class is called for every endpoint call.
    """

    def __init__(self, cluster_name: str, local_mode_address: str):
        self.cluster_name = cluster_name
        self.cluster_address = os.environ[local_mode_address] if LOCAL_MODE else os.environ["DASK_GATEWAY_ADDRESS"]
        self.cluster_instance = ""
        self.cluster: GatewayCluster

    def _connect_to_cluster(self):
        """
        Handles the first part of setup_dask_connection.
        See there for details.
        """
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
            # This cluster instance id is needed by the eopf dask scheduler to connect later to this cluster.
            # This is something like "dask-gateway.17e196069443463495547eb97f532834"
            if LOCAL_MODE:
                if clusters:
                    self.cluster_instance = clusters[0].name

            # In cluster mode, get the identifier of the cluster whose name is equal to the cluster_name variable.
            # Protection for the case when this cluster does not exit
            else:
                logger.info(f"my cluster name: {self.cluster_name}")

                for cluster in clusters:
                    logger.info(f"Existing cluster names: {cluster.options.get('cluster_name')}")

                    is_equal = cluster.options.get("cluster_name") == self.cluster_name
                    logger.info(f"Is equal: {is_equal}")

                self.cluster_instance = next(
                    (
                        cluster.name
                        for cluster in clusters
                        if isinstance(cluster.options, dict)
                        and cluster.options.get("cluster_name") == self.cluster_name
                    ),
                    "",
                )
                logger.info(f"Cluster id: {self.cluster_instance}")

            if not self.cluster_instance:
                raise IndexError(f"Dask cluster with 'cluster_name'={self.cluster_name!r} was not found.")

            self.cluster = gateway.connect(self.cluster_instance)
            if not self.cluster:
                logger.exception("Failed to create the cluster")
                raise RuntimeError("Failed to create the cluster")
            logger.info(f"Successfully connected to the {self.cluster_name!r} dask cluster")

        except KeyError as e:
            logger.exception(
                "Failed to retrieve the required connection details for "
                "the Dask Gateway from one or more of the following environment variables: "
                "DASK_GATEWAY_ADDRESS, RSPY_DASK_DPR_SERVICE_CLUSTER_NAME, "
                f"JUPYTERHUB_API_TOKEN, DASK_GATEWAY__AUTH__TYPE. {e}",
            )

            raise RuntimeError(
                f"Failed to retrieve the required connection details for Dask Gateway. Missing key:{e}",
            ) from e
        except IndexError as e:
            logger.exception(f"Failed to find the specified dask cluster: {e}")
            raise RuntimeError(f"No dask cluster named {self.cluster_name!r} was found.") from e

    def setup_dask_connection(self) -> Client:
        """Connects a dask cluster scheduler
        Establishes a connection to a Dask cluster, either in a local environment or via a Dask Gateway in
        a Kubernetes cluster. This method checks if the cluster is already created (for local mode) or connects
        to a Dask Gateway to find or create a cluster scheduler (for Kubernetes mode, see RSPY_LOCAL_MODE env var).

        1. **Local Mode**:
        - If `self.cluster` already exists, it assumes the Dask cluster was created when the application started,
            and proceeds without creating a new cluster.

        2. **Kubernetes Mode**:
        - If `self.cluster` is not already defined, the method attempts to connect to a Dask Gateway
            (using environment variables `DASK_GATEWAY_ADDRESS` and `DASK_GATEWAY__AUTH__TYPE`) to
            retrieve a list of existing clusters.
        - If no clusters are available, it attempts to create a new cluster scheduler.

        Raises:
            RuntimeError: Raised if the cluster name is None, required environment variables are missing,
                        cluster creation fails or authentication errors occur.
            KeyError: Raised if the necessary Dask Gateway environment variables (`DASK_GATEWAY_ADDRESS`,
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
        self._connect_to_cluster()

        logger.debug("Cluster dashboard: %s", self.cluster.dashboard_link)
        # create the client as well
        client = Client(self.cluster)

        # Forward logging from dask workers to the caller
        client.forward_logging()

        # Upload local module to the dask client.
        call_dask.upload_this_module(client)

        # set_dask_env function is in utils, uploaded to the dask cluster in call_dask
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
