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

"""Advanced logger for Dask jobs."""

import uuid
from datetime import datetime

from pygeoapi.process.manager.postgresql import (
    PostgreSQLManager,  # pylint: disable=C0302
)
from pygeoapi.util import JobStatus

from rs_dpr_service.utils.logging import Logging

logger = Logging.default(__name__)


class JobLogger:
    """
    Advanced logger to track Dask jobs execution.
    """

    def __init__(self, db_process_manager: PostgreSQLManager):
        self.job_id: str = str(uuid.uuid4())  # Generate a unique job ID
        self.message: str = "Processing Unit was created"
        self.progress: float = 0.0
        self.status = JobStatus.accepted
        self.db_process_manager = db_process_manager
        self.create_job_execution()

    def get_execute_result(self) -> tuple[str, dict]:
        """
        Returns current status of the job logger.

        Returns:
            tuple: tuple of MIME type and process response (dictionary containing the job ID and a
                status message).
                Example: ("application/json", {"running": <job_id>})
        """
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
        log_exception: bool = False,
    ) -> tuple[str, dict]:
        """
        Method used to log progress into db.

        Args:
            status (JobStatus): new job status
            progress (int): new job progress (percentage)
            message (str): new job current information message
            log_exception (bool): log.exception the message

        Returns:
            tuple: tuple of MIME type and process response (dictionary containing the job ID and a
                status message).
                Example: ("application/json", {"running": <job_id>})
        """

        # Update both runtime and db status and progress
        self.status = status if status else self.status
        self.progress = progress if progress else self.progress
        self.message = message if message else self.message

        if log_exception:
            logger.exception(self.message)

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
