from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from nexus.otel_setup import TelemetryConfig, configure_otel, inject_trace_headers, message_span  # noqa: E402


__all__ = ["TelemetryConfig", "configure_otel", "inject_trace_headers", "message_span"]


if __name__ == "__main__":
    configure_otel(TelemetryConfig(service_name="nexus.agent.researcher"))