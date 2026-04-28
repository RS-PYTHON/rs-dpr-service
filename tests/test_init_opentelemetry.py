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

"""Tests for rs_dpr_service.utils.init_opentelemetry."""

from contextlib import contextmanager
import sys
import types

import pytest
import requests
from opentelemetry.trace.span import NonRecordingSpan, SpanContext, TraceFlags

from rs_dpr_service.utils import init_opentelemetry
from rs_dpr_service.utils.init_opentelemetry import (
    fastapi_hook,
    init_traces,
    parse_data,
    record_error,
    requests_hook,
    start_span,
    trace_body,
    trace_headers,
)


def test_trace_flags_read_environment_variables(monkeypatch):
    """Test trace_headers() and trace_body() read their environment flags."""
    monkeypatch.setenv("OTEL_PYTHON_REQUESTS_TRACE_HEADERS", "true")
    monkeypatch.setenv("OTEL_PYTHON_REQUESTS_TRACE_BODY", "1")

    assert trace_headers() is True
    assert trace_body() is True


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        (None, ""),
        (b"plain text", "plain text"),
        ('{"key": "value"}', '{\n  "key": "value"\n}'),
        ({b"key": b"value"}, '{\n  "key": "value"\n}'),
        ([("key", "value")], '{\n  "key": "value"\n}'),
    ],
)
def test_parse_data_formats_supported_payloads(data, expected):
    """Test parse_data() converts headers and bodies to stable strings."""
    assert parse_data(data) == expected


def test_requests_hook_adds_headers_and_body_attributes(mocker, monkeypatch):
    """Test requests_hook() enriches a recording span with request and response data."""
    monkeypatch.setenv("OTEL_PYTHON_REQUESTS_TRACE_HEADERS", "true")
    monkeypatch.setenv("OTEL_PYTHON_REQUESTS_TRACE_BODY", "true")

    span = mocker.Mock()
    span.is_recording.return_value = True
    span.attributes = {"http.url": "https://example.test/data"}
    request = requests.Request(
        "POST",
        "https://example.test/data",
        headers={"X-Test": "yes"},
        data="request-body",
    ).prepare()
    response = requests.Response()
    response.status_code = 200
    response.headers["X-Response"] = "ok"
    # requests_hook() reads response.content, so mark the fake response body as already loaded.
    response._content = b"response-body"  # pylint: disable=protected-access
    setattr(response, "_content_consumed", True)

    requests_hook(span, request, response)

    span.set_attribute.assert_any_call("_url", "https://example.test/data")
    span.set_attribute.assert_any_call("http.request.headers", '{\n  "X-Test": "yes",\n  "Content-Length": "12"\n}')
    span.set_attribute.assert_any_call("http.response.headers", '{\n  "X-Response": "ok"\n}')
    span.set_attribute.assert_any_call("http.request.body", "request-body")
    span.set_attribute.assert_any_call("http.response.content", "response-body")


def test_requests_hook_ignores_non_recording_span(mocker):
    """Test requests_hook() returns early when the span is not recording."""
    span = mocker.Mock()
    span.is_recording.return_value = False
    request = requests.Request("GET", "https://example.test").prepare()

    requests_hook(span, request)

    span.set_attribute.assert_not_called()


def test_fastapi_hook_adds_scope_and_message_attributes(mocker, monkeypatch):
    """Test fastapi_hook() enriches a recording span with ASGI scope and message data."""
    monkeypatch.setenv("OTEL_PYTHON_REQUESTS_TRACE_HEADERS", "true")
    monkeypatch.setenv("OTEL_PYTHON_REQUESTS_TRACE_BODY", "true")

    span = mocker.Mock()
    span.is_recording.return_value = True
    # ASGI headers are byte pairs; parse_data() decodes them before attaching span attributes.
    scope = {"path": "/dpr/processes", "headers": [(b"x-test", b"yes")]}
    message = {"headers": [(b"x-message", b"ok")], "body": b"message-body"}

    fastapi_hook(span, scope, message)

    span.set_attribute.assert_any_call("_path", "/dpr/processes")
    span.set_attribute.assert_any_call("http.scope.headers", '{\n  "x-test": "yes"\n}')
    span.set_attribute.assert_any_call("http.message.headers", '{\n  "x-message": "ok"\n}')
    span.set_attribute.assert_any_call("http.message.body", "message-body")


def test_fastapi_hook_ignores_non_recording_span(mocker):
    """Test fastapi_hook() returns early when the span is not recording."""
    span = mocker.Mock()
    span.is_recording.return_value = False

    fastapi_hook(span, {"path": "/dpr/processes"})

    span.set_attribute.assert_not_called()


def test_start_span_creates_root_span(mocker):
    """Test start_span() creates a root span when no parent context is provided."""
    expected_span = mocker.Mock()

    @contextmanager
    def fake_start_as_current_span(name):
        """Yield a fake span from the mocked tracer."""
        assert name == "root-span"
        yield expected_span

    tracer = mocker.Mock()
    tracer.start_as_current_span.side_effect = fake_start_as_current_span
    get_tracer = mocker.patch("rs_dpr_service.utils.init_opentelemetry.trace.get_tracer", return_value=tracer)

    with start_span("test.module", "root-span") as span:
        assert span is expected_span

    get_tracer.assert_called_once_with("test.module")
    tracer.start_as_current_span.assert_called_once_with("root-span")


def test_start_span_creates_child_span_from_parent_context(mocker):
    """Test start_span() creates a child span when a parent context is provided."""
    expected_span = mocker.Mock()
    parent_context = SpanContext(
        trace_id=1,
        span_id=2,
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )

    @contextmanager
    def fake_start_as_current_span(name):
        """Yield a fake child span from the mocked tracer."""
        assert name == "child-span"
        yield expected_span

    @contextmanager
    def fake_use_span(span):
        """Assert the parent span wrapper is used around the child span."""
        assert isinstance(span, NonRecordingSpan)
        assert span.get_span_context().trace_id == parent_context.trace_id
        assert span.get_span_context().span_id == parent_context.span_id
        yield span

    tracer = mocker.Mock()
    tracer.start_as_current_span.side_effect = fake_start_as_current_span
    mocker.patch("rs_dpr_service.utils.init_opentelemetry.trace.get_tracer", return_value=tracer)
    # Child spans first bind a NonRecordingSpan made from the parent SpanContext.
    use_span = mocker.patch("rs_dpr_service.utils.init_opentelemetry.trace.use_span", side_effect=fake_use_span)

    with start_span("test.module", "child-span", parent_context) as span:
        assert span is expected_span

    use_span.assert_called_once()
    tracer.start_as_current_span.assert_called_once_with("child-span")


@pytest.mark.parametrize("is_recording", [False, True])
def test_record_error_records_exception_only_when_span_is_recording(mocker, is_recording):
    """Test record_error() records exceptions only for recording spans."""
    span = mocker.Mock()
    span.is_recording.return_value = is_recording
    error = RuntimeError("processor failed")

    record_error(span, error)

    span.is_recording.assert_called_once_with()
    if is_recording:
        span.record_exception.assert_called_once_with(error)
        span.set_status.assert_called_once()
    else:
        span.record_exception.assert_not_called()
        span.set_status.assert_not_called()


def test_init_traces_configures_fastapi_and_instrumentors(mocker, monkeypatch):
    """Test init_traces() configures tracing, FastAPI hooks, and discovered instrumentors."""
    monkeypatch.setenv("TEMPO_ENDPOINT", "http://tempo:4317")
    monkeypatch.setenv("OTEL_PYTHON_REQUESTS_TRACE_HEADERS", "true")
    monkeypatch.setenv("OTEL_PYTHON_REQUESTS_TRACE_BODY", "true")
    # Force the production branch without exporting anything real; exporters are mocked below.
    monkeypatch.setattr(init_opentelemetry, "FROM_PYTEST", False)

    class FakeRequestsInstrumentor:
        """Fake Requests instrumentor used to assert hook registration."""

        is_instrumented_by_opentelemetry = False
        instrument = mocker.Mock()

    class FakeFastAPIInstrumentor:
        """Fake FastAPI instrumentor used for app and discovered instrumentation."""

        is_instrumented_by_opentelemetry = False
        instrument_app = mocker.Mock()
        instrument = mocker.Mock()

    class FakeGeneralInstrumentor:
        """Fake generic instrumentor used to cover the no-hook branch."""

        is_instrumented_by_opentelemetry = False
        instrument = mocker.Mock()

    class FakeAlreadyInstrumented:
        """Fake instrumentor used to cover the already-instrumented branch."""

        is_instrumented_by_opentelemetry = True
        instrument = mocker.Mock()

    fake_module_name = "opentelemetry.instrumentation.fake"
    fake_module = types.ModuleType(fake_module_name)
    setattr(fake_module, "RequestsInstrumentor", FakeRequestsInstrumentor)
    setattr(fake_module, "FastAPIInstrumentor", FakeFastAPIInstrumentor)
    setattr(fake_module, "FakeGeneralInstrumentor", FakeGeneralInstrumentor)
    setattr(fake_module, "FakeAlreadyInstrumented", FakeAlreadyInstrumented)
    setattr(fake_module, "AsyncioInstrumentor", init_opentelemetry.AsyncioInstrumentor)
    monkeypatch.setitem(sys.modules, fake_module_name, fake_module)

    # init_traces() discovers instrumentors by importing modules returned by pkgutil.walk_packages().
    def fake_import(name, *args, **kwargs):
        """Return the fake module during instrumentation discovery."""
        if name == fake_module_name:
            return fake_module
        return original_import(name, *args, **kwargs)

    original_import = __import__
    monkeypatch.setattr("builtins.__import__", fake_import)
    mocker.patch(
        "rs_dpr_service.utils.init_opentelemetry.pkgutil.walk_packages",
        return_value=[
            (None, "opentelemetry.instrumentation.tortoiseorm", False),
            (None, fake_module_name, False),
        ],
    )
    mocker.patch("rs_dpr_service.utils.init_opentelemetry.RequestsInstrumentor", FakeRequestsInstrumentor)
    mocker.patch("rs_dpr_service.utils.init_opentelemetry.FastAPIInstrumentor", FakeFastAPIInstrumentor)

    # Mock provider/exporter classes so no global OpenTelemetry exporter is started.
    tracer_provider = mocker.Mock()
    tracer_provider.add_span_processor = mocker.Mock()
    otel_mocks = {
        "tracer_provider_cls": mocker.patch(
            "rs_dpr_service.utils.init_opentelemetry.TracerProvider",
            return_value=tracer_provider,
        ),
        "set_tracer_provider": mocker.patch("rs_dpr_service.utils.init_opentelemetry.trace.set_tracer_provider"),
        "batch_span_processor": mocker.patch("rs_dpr_service.utils.init_opentelemetry.BatchSpanProcessor"),
        "otlp_exporter": mocker.patch("rs_dpr_service.utils.init_opentelemetry.OTLPSpanExporter"),
    }

    app = mocker.Mock()

    init_traces(app, "rs.dpr.service", logger=mocker.Mock())

    otel_mocks["tracer_provider_cls"].assert_called_once()
    otel_mocks["set_tracer_provider"].assert_called_once_with(tracer_provider)
    otel_mocks["otlp_exporter"].assert_called_once_with(endpoint="http://tempo:4317")
    otel_mocks["batch_span_processor"].assert_called_once_with(otel_mocks["otlp_exporter"].return_value)
    tracer_provider.add_span_processor.assert_called_once_with(otel_mocks["batch_span_processor"].return_value)
    FakeFastAPIInstrumentor.instrument_app.assert_called_once_with(
        app,
        tracer_provider=tracer_provider,
        server_request_hook=fastapi_hook,
        client_request_hook=fastapi_hook,
        client_response_hook=fastapi_hook,
    )
    FakeRequestsInstrumentor.instrument.assert_called_once_with(
        tracer_provider=tracer_provider,
        request_hook=requests_hook,
        response_hook=requests_hook,
    )
    FakeFastAPIInstrumentor.instrument.assert_called_once_with(
        tracer_provider=tracer_provider,
        server_request_hook=fastapi_hook,
        client_request_hook=fastapi_hook,
        client_response_hook=fastapi_hook,
    )
    FakeGeneralInstrumentor.instrument.assert_called_once_with(tracer_provider=tracer_provider)
    FakeAlreadyInstrumented.instrument.assert_not_called()
