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

import asyncio
import logging
import re
import threading
from unittest.mock import Mock, patch

import pytest

from rs_dpr_service.utils.logging import EOPF_OUTPUT_LOG_ATTR, JobLogHandler


def make_record(message: str, level: int = logging.INFO, eopf_output: bool = True) -> logging.LogRecord:
    """Build a minimal log record for JobLogHandler. By default, the record contains raw EOPF output."""
    record = logging.LogRecord(
        name="rs_dpr_service.dask.call_dask",
        level=level,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )
    if eopf_output:
        setattr(record, EOPF_OUTPUT_LOG_ATTR, True)
    return record


def test_job_log_handler_prefixes_rs_dpr_service_logs_with_date_logger_and_level():
    """The rs-dpr-service own logs keep their date, logger name and level, in the same format as EOPF."""
    handler = JobLogHandler()
    record = make_record("[JOB:job-1] Call 'ProcessorCaller.__init__' from '10.2.9.2'", logging.DEBUG, False)

    handler.emit(record)

    [(index, message)] = handler.get_logs("job-1")
    assert index == 0
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - rs_dpr_service\.dask\.call_dask - DEBUG - "
        r"Call 'ProcessorCaller\.__init__' from '10\.2\.9\.2'",
        message,
    )


def test_job_log_handler_keeps_eopf_output_unchanged():
    """The EOPF output lines already have their own prefix, they are sent as they are."""
    handler = JobLogHandler()
    eopf_lines = (
        "2026-10-28 08:29:14,634 - l0.s3.s3_l0_processor - INFO - Reading ADF\n"
        "2026-10-28 08:29:14,635 - l0.s3.s3_l0_processor - WARNING - Unable to get dump granule position"
    )

    handler.emit(make_record(f"[JOB:job-1] {eopf_lines}"))

    assert handler.get_logs("job-1") == [(0, eopf_lines)]


def test_job_log_handler_buffers_job_logs_by_job_id():
    """Job logs are stripped from their marker and buffered per job, with increasing indexes."""
    handler = JobLogHandler()

    handler.emit(make_record("[JOB:job-1] Processing started"))
    handler.emit(make_record("[JOB:job-2] Other job"))
    handler.emit(make_record("[JOB:job-1] Processing finished"))

    assert handler.get_logs("job-1") == [(0, "Processing started"), (1, "Processing finished")]
    assert handler.get_logs("job-1", 1) == [(1, "Processing finished")]
    assert handler.get_logs("job-1", 2) == []
    assert handler.get_logs("job-2") == [(0, "Other job")]
    assert handler.get_logs("unknown-job") == []


def test_job_log_handler_ignores_logs_without_job_marker():
    """Logs that do not contain a job marker are not buffered."""
    handler = JobLogHandler()

    handler.emit(make_record("Processing started"))

    assert not handler.job_ids


def test_job_log_handler_preserves_multiline_job_log_payload():
    """The payload can span multiple lines after the job marker."""
    handler = JobLogHandler()

    handler.emit(make_record("[JOB:job-1] First line\nSecond line"))

    assert handler.get_logs("job-1") == [(0, "First line\nSecond line")]


def test_job_log_handler_memory_is_bounded():
    """The number of jobs and of messages per job are bounded. Indexes keep increasing."""
    handler = JobLogHandler(max_jobs=2, max_messages_per_job=2)

    for i in range(3):
        handler.emit(make_record(f"[JOB:job-1] log {i}"))
    handler.emit(make_record("[JOB:job-2] log"))
    handler.emit(make_record("[JOB:job-3] log"))

    assert handler.job_ids == ["job-2", "job-3"]

    handler.emit(make_record("[JOB:job-2] log"))
    for i in range(3):
        handler.emit(make_record(f"[JOB:job-4] log {i}"))
    assert handler.job_ids == ["job-2", "job-4"]
    assert handler.get_logs("job-4") == [(1, "log 1"), (2, "log 2")]
    assert handler.get_logs("job-4", 0) == [(1, "log 1"), (2, "log 2")]


@pytest.mark.asyncio
async def test_job_log_handler_does_not_evict_jobs_with_subscribers():
    """Jobs with an active SSE subscriber are not evicted."""
    handler = JobLogHandler(max_jobs=1)
    event = handler.subscribe("job-1")

    handler.emit(make_record("[JOB:job-2] log"))
    assert handler.job_ids == ["job-1", "job-2"]

    handler.unsubscribe("job-1", event)
    handler.emit(make_record("[JOB:job-3] log"))
    assert handler.job_ids == ["job-3"]


@pytest.mark.asyncio
async def test_job_log_handler_wakes_up_subscribers_from_another_thread():
    """Logs are emitted by the dask client IO thread: subscribers must be woken up thread-safely."""
    handler = JobLogHandler()
    event = handler.subscribe("job-1")
    other_event = handler.subscribe("job-2")

    thread = threading.Thread(target=handler.emit, args=(make_record("[JOB:job-1] from thread"),))
    thread.start()
    await asyncio.wait_for(event.wait(), timeout=5)
    thread.join()

    assert handler.get_logs("job-1") == [(0, "from thread")]
    assert not other_event.is_set()


def test_job_log_handler_delegates_unexpected_errors_to_handle_error():
    """Unexpected errors are delegated to logging.Handler.handleError."""
    handler = JobLogHandler()
    record = make_record("[JOB:job-1] Processing started")

    with (
        patch.object(handler, "format", Mock(side_effect=RuntimeError("format error"))),
        patch.object(handler, "handleError") as handle_error,
    ):
        handler.emit(record)

    handle_error.assert_called_once_with(record)
