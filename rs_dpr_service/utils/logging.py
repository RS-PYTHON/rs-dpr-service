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

"""Logging utility."""

import asyncio
import logging
import re
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from threading import Lock


class Logging:  # pylint: disable=too-few-public-methods
    """
    Logging utility.

    Attributes:
        lock: For code synchronization
        level: Minimal log level to use for all new logging instances.
    """

    lock = Lock()
    level = logging.DEBUG

    _HANDLER_NAME = "rspy-console-handler"

    @classmethod
    def default(cls, name="rspy"):
        """
        Return a default Logger class instance.

        Args:
            name (str): Logger name. You can pass __name__ to use your current module name.
        """
        logger = logging.getLogger(name=name)

        with cls.lock:
            # Don't propagate to root logger
            logger.propagate = False

            # If we have already set our handler for the logger with this name, do nothing more
            for handler in logger.handlers:
                if getattr(handler, "name", None) == cls._HANDLER_NAME:
                    return logger

            # Set the minimal log level to use for all new logging instances.
            logger.setLevel(cls.level)

            # Create console handler
            handler = logging.StreamHandler()
            handler.setFormatter(CustomFormatter())
            handler.name = cls._HANDLER_NAME
            logger.addHandler(handler)

            return logger


class CustomFormatter(logging.Formatter):
    """
    Custom logging formatter with colored text.
    See: https://stackoverflow.com/a/56944256
    """

    _RED = "\x1b[31m"
    _BOLD_RED = "\x1b[31;1m"
    _GREEN = "\x1b[32m"
    _YELLOW = "\x1b[33m"
    _PURPLE = "\x1b[35m"
    _RESET = "\x1b[0m"

    _FORMAT = (
        f"%(asctime)s.%(msecs)03d {{color}}%(levelname)s{_RESET} [trace_id=%(otelTraceID)s span_id=%(otelSpanID)s"
        " resource.service.name=%(otelServiceName)s trace_sampled=%(otelTraceSampled)s] (%(name)s) %(message)s"
    )
    _DATETIME = "%H:%M:%S"

    _FORMATS = {
        logging.NOTSET: _FORMAT.format(color=""),
        logging.DEBUG: _FORMAT.format(color=_PURPLE),
        logging.INFO: _FORMAT.format(color=_GREEN),
        logging.WARNING: _FORMAT.format(color=_YELLOW),
        logging.ERROR: _FORMAT.format(color=_BOLD_RED),
        logging.CRITICAL: _FORMAT.format(color=_RED),
    }

    def format(self, record):

        # Set default OpenTelemetry values if missing
        for key in "otelTraceID", "otelSpanID", "otelServiceName", "otelTraceSampled":
            if key not in record.__dict__:
                record.__dict__[key] = None

        level_format = self._FORMATS.get(record.levelno)
        formatter = logging.Formatter(level_format, self._DATETIME)
        return formatter.format(record)


@dataclass
class _JobLogs:
    """Buffered log messages of a single job."""

    messages: deque[str]
    # Absolute index of messages[0] (> 0 when the oldest messages were dropped)
    first_index: int = 0
    waiters: set[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = field(default_factory=set)


class JobLogHandler(logging.Handler):
    """
    Custom log handler that stores Dask worker logs in per job buffers for SSE streaming.

    emit() is called from the dask Client IO thread (see dask_client.forward_logging), not from the
    asyncio event loop serving the SSE endpoint. asyncio objects are not thread-safe, so the buffers
    are protected by a lock and the SSE subscribers are woken up with loop.call_soon_threadsafe.

    Logs are kept in memory (bounded) even with no subscriber, so a client connecting late or
    reconnecting after a network error can resume from its last received message index.
    """

    def __init__(self, max_jobs: int = 100, max_messages_per_job: int = 20_000):
        super().__init__()
        self.max_jobs = max_jobs
        self.max_messages_per_job = max_messages_per_job
        self._jobs: OrderedDict[str, _JobLogs] = OrderedDict()
        self._lock = Lock()

    def _get_or_create(self, job_id: str) -> _JobLogs:
        """Return the job logs, create them if missing. Must be called with the lock acquired."""
        job_logs = self._jobs.get(job_id)
        if job_logs is None:
            job_logs = _JobLogs(messages=deque(maxlen=self.max_messages_per_job))
            self._jobs[job_id] = job_logs
            # Evict the oldest jobs that nobody is listening to
            for old_id in list(self._jobs):
                if len(self._jobs) <= self.max_jobs:
                    break
                if old_id != job_id and not self._jobs[old_id].waiters:
                    del self._jobs[old_id]
        else:
            self._jobs.move_to_end(job_id)
        return job_logs

    def emit(self, record):
        try:
            msg = self.format(record)
            match = re.search(r"\[JOB:([^\]]+)\]\s*(.*)", msg, re.DOTALL)
            if not match:
                return
            job_id = match.group(1)
            clean_msg = match.group(2)
            with self._lock:
                job_logs = self._get_or_create(job_id)
                if len(job_logs.messages) == job_logs.messages.maxlen:
                    job_logs.first_index += 1
                job_logs.messages.append(clean_msg)
                waiters = list(job_logs.waiters)
            for loop, event in waiters:
                try:
                    loop.call_soon_threadsafe(event.set)
                except RuntimeError:  # the event loop is closed
                    pass
        except Exception:  # pylint: disable=broad-exception-caught
            self.handleError(record)

    def get_logs(self, job_id: str, start: int = 0) -> list[tuple[int, str]]:
        """Return the (index, message) of the buffered job logs with an index >= start."""
        with self._lock:
            job_logs = self._jobs.get(job_id)
            if job_logs is None:
                return []
            skip = max(0, start - job_logs.first_index)
            return [(job_logs.first_index + i, message) for i, message in enumerate(job_logs.messages) if i >= skip]

    def subscribe(self, job_id: str) -> asyncio.Event:
        """
        Return an asyncio Event that is set each time a new log is received for the job.
        Must be called from the event loop that will wait on the Event.
        """
        event = asyncio.Event()
        with self._lock:
            self._get_or_create(job_id).waiters.add((asyncio.get_running_loop(), event))
        return event

    def unsubscribe(self, job_id: str, event: asyncio.Event):
        """Unregister an Event returned by subscribe()"""
        with self._lock:
            job_logs = self._jobs.get(job_id)
            if job_logs is not None:
                job_logs.waiters = {waiter for waiter in job_logs.waiters if waiter[1] is not event}

    def clear(self, job_id: str):
        """Remove the buffered logs of a job."""
        with self._lock:
            self._jobs.pop(job_id, None)

    @property
    def job_ids(self) -> list[str]:
        """Return the IDs of the jobs that have buffered logs."""
        with self._lock:
            return list(self._jobs)
