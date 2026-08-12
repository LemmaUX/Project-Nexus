from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping

try:
    from opentelemetry import trace as trace_api
    from opentelemetry.context import attach, detach
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    _HAS_OPENTELEMETRY = True
except ImportError:  # pragma: no cover - optional observability dependency
    trace_api = None
    attach = None
    detach = None
    OTLPSpanExporter = None
    Resource = None
    TracerProvider = None
    BatchSpanProcessor = None
    TraceContextTextMapPropagator = None
    _HAS_OPENTELEMETRY = False


@dataclass(frozen=True)
class TelemetryConfig:
    service_name: str
    otlp_endpoint: str = "http://localhost:4317"
    insecure: bool = True
    resource_attributes: Mapping[str, str] | None = None


class _NoopSpan:
    def set_status(self, *_: Any, **__: Any) -> None:
        return None

    def record_exception(self, *_: Any, **__: Any) -> None:
        return None


class _NoopTracer:
    @contextmanager
    def start_as_current_span(self, *_: Any, **__: Any):
        yield _NoopSpan()


class _NoopPropagator:
    @staticmethod
    def extract(carrier: Mapping[str, Any] | None = None, context: Any | None = None) -> Any:
        return context

    @staticmethod
    def inject(carrier: MutableMapping[str, str] | None = None, context: Any | None = None) -> None:
        return None


_PROPAGATOR = TraceContextTextMapPropagator() if _HAS_OPENTELEMETRY else _NoopPropagator()
_TRACERS: dict[str, Any] = {}
_TRACER_PROVIDER_CONFIGURED = False


def configure_otel(config: TelemetryConfig) -> Any:
    global _TRACER_PROVIDER_CONFIGURED

    cached = _TRACERS.get(config.service_name)
    if cached is not None:
        return cached

    if not _HAS_OPENTELEMETRY:
        tracer = _NoopTracer()
        _TRACERS[config.service_name] = tracer
        return tracer

    attributes = {
        "service.name": config.service_name,
        **(dict(config.resource_attributes) if config.resource_attributes else {}),
    }
    if not _TRACER_PROVIDER_CONFIGURED:
        provider = TracerProvider(resource=Resource.create(attributes))
        exporter = OTLPSpanExporter(endpoint=config.otlp_endpoint, insecure=config.insecure)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace_api.set_tracer_provider(provider)
        _TRACER_PROVIDER_CONFIGURED = True

    tracer = trace_api.get_tracer(config.service_name)
    _TRACERS[config.service_name] = tracer
    return tracer


def extract_trace_context(headers: Mapping[str, Any] | None) -> Any:
    if not headers:
        return None
    carrier = {
        "traceparent": str(headers.get("Traceparent", headers.get("traceparent", ""))),
        "tracestate": str(headers.get("Tracestate", headers.get("tracestate", ""))),
    }
    return _PROPAGATOR.extract(carrier=carrier)


def inject_trace_headers() -> dict[str, str]:
    carrier: dict[str, str] = {}
    _PROPAGATOR.inject(carrier=carrier)
    return {
        "Traceparent": carrier.get("traceparent", ""),
        "Tracestate": carrier.get("tracestate", ""),
    }


@contextmanager
def message_span(
    service_name: str,
    span_name: str,
    incoming_headers: Mapping[str, Any] | None = None,
    attributes: Mapping[str, Any] | None = None,
    kind: Any | None = None,
):
    tracer = configure_otel(TelemetryConfig(service_name=service_name))
    context = extract_trace_context(incoming_headers)
    token = attach(context) if _HAS_OPENTELEMETRY and context is not None else None
    try:
        span_kwargs: dict[str, Any] = {}
        if attributes:
            span_kwargs["attributes"] = dict(attributes)
        if kind is not None:
            span_kwargs["kind"] = kind
        with tracer.start_as_current_span(span_name, **span_kwargs) as span:
            yield span
    finally:
        if _HAS_OPENTELEMETRY and token is not None:
            detach(token)