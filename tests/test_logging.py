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

"""Tests for logging utilities."""

import logging
from asyncio import Queue
from unittest.mock import Mock, patch

from rs_dpr_service.utils.logging import JobLogHandler


def make_record(message: str) -> logging.LogRecord:
    """Build a minimal log record for JobLogHandler."""
    return logging.LogRecord(
        name="rs_dpr_service.dask.call_dask",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )


def test_job_log_handler_routes_job_log_to_all_matching_queues():
    """Job logs are stripped from their marker and sent to every queue registered for the job."""
    handler = JobLogHandler()
    first_queue: Queue[str] = Queue()
    second_queue: Queue[str] = Queue()
    other_job_queue: Queue[str] = Queue()
    handler.queues["job-1"] = [first_queue, second_queue]
    handler.queues["job-2"] = [other_job_queue]

    handler.emit(make_record("[JOB:job-1] Processing started"))

    assert first_queue.get_nowait() == "Processing started"
    assert second_queue.get_nowait() == "Processing started"
    assert other_job_queue.empty()


def test_job_log_handler_does_not_leak_an_entry_for_jobs_with_no_subscriber():
    """Emitting a job log for a job with no registered queue must not create a dict entry.

    self.queues used to be a defaultdict(list), so reading self.queues[job_id] for an
    unsubscribed job silently created and kept an empty list forever: a leak that grows for
    the whole pod lifetime, one entry per job, since most jobs are never tailed over SSE.
    """
    handler = JobLogHandler()

    handler.emit(make_record("[JOB:job-without-subscriber] Processing started"))

    assert not handler.queues


def test_job_log_handler_ignores_logs_without_job_marker():
    """Logs that do not contain a job marker are not routed to any queue."""
    handler = JobLogHandler()
    queue: Queue[str] = Queue()
    handler.queues["job-1"] = [queue]

    handler.emit(make_record("Processing started"))

    assert queue.empty()


def test_job_log_handler_preserves_multiline_job_log_payload():
    """The payload can span multiple lines after the job marker."""
    handler = JobLogHandler()
    queue: Queue[str] = Queue()
    handler.queues["job-1"] = [queue]

    handler.emit(make_record("[JOB:job-1] First line\nSecond line"))

    assert queue.get_nowait() == "First line\nSecond line"


def test_job_log_handler_delegates_unexpected_errors_to_handle_error():
    """Unexpected queue errors are delegated to logging.Handler.handleError."""
    handler = JobLogHandler()
    failing_queue = Mock()
    failing_queue.put_nowait.side_effect = RuntimeError("queue is unavailable")
    handler.queues["job-1"] = [failing_queue]
    record = make_record("[JOB:job-1] Processing started")

    with patch.object(handler, "handleError") as handle_error:
        handler.emit(record)

    handle_error.assert_called_once_with(record)
