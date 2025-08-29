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

"""Store diverse objects and values used throughout the application."""

import os

from pydantic import BaseModel


class ExperimentalConfig(BaseModel):
    """Experimental configuration, used only for testing."""

    class LocalCluster(BaseModel):
        """
        Overwrite the payload file to use a Dask LocalCluster configuration instead of a Dask Gateway.

        If disabled (by default), we use the Dask Gateway cluster and workers that have been initialized by RSPY.

        If enabled in cluster mode, a nested Dask LocalCluster is initialized by EOPF inside the RSPY Dask Gateway.
        The Dask Gateway should be set with a single worker, else we face unexpected behaviour. The EOPF LocalCluster
        will run inside this single worker.

        If enabled in local mode, the RSPY Dask Gateway is not used. We use only the EOPF LocalCluster. Your local mode
        should use the docker image ghcr.io/rs-python/dask-gateway-server/eopf/localcluster and not
        ghcr.io/rs-python/rs-dpr-service because it contains both the rs-dpr-service and the processor source code
        and dependencies. We use this mode to be able to debug and put breakpoints in the EOPF and processor source
        code.
        """

        enabled: bool = False  # Use False to disable

        #
        # Dask LocalCluster configuration, see: https://distributed.dask.org/en/latest/api.html#distributed.LocalCluster

        # Number of workers to start. Default is CPU_COUNT.
        n_workers: int | None = None

        # Sets the memory limit *per worker*
        memory_limit: str | float | int | None = "auto"

        # Number of threads per each worker.
        # Should always be 1 because the processors are not thread-safe.
        threads_per_worker: int = 1

    class LocalFiles(BaseModel):
        """
        Overwrite the payload file to read/write on the local disk rather than on the S3 bucket.
        Only works with a LocalCluster.
        """

        # Local directory
        local_dir: str | None = None  # Use None to disable

        # Download input files again from the S3 bucket if they are already present on the local directory ?
        overwrite_input: bool = False

        # Upload output files to the S3 bucket ?
        upload_output: bool = False

    local_cluster: LocalCluster = LocalCluster()
    local_files: LocalFiles = LocalFiles()


#########################
# Environment variables #
#########################


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
