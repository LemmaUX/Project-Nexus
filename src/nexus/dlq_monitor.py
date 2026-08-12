from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

import httpx

from .otel_setup import TelemetryConfig, configure_otel, message_span

logger = logging.getLogger("nexus.dlq.monitor")


@dataclass(frozen=True)
class DLQMonitorConfig:
    nats_url: str = "nats://localhost:4222"
    alert_webhook: str = ""
    subject: str = "nexus.dlq.agent.>"
    stream_name: str = "NEXUS_DLQ"
    durable_name: str = "dlq-monitor-durable"
    batch_size: int = 10
    fetch_timeout_seconds: float = 10.0


class DLQMonitor:
    """Triage JetStream DLQ messages and emit structured alerts."""

    def __init__(self, config: DLQMonitorConfig | None = None) -> None:
        self.config = config or DLQMonitorConfig()
        configure_otel(TelemetryConfig(service_name="nexus.dlq.monitor"))
        self.nc: Any = None
        self.js: Any = None
        self.sub: Any = None

    async def start(self) -> None:
        try:
            from nats.aio.client import Client as NATS
            from nats.js.api import ConsumerConfig, DeliverPolicy
        except ImportError as exc:  # pragma: no cover - optional runtime dependency
            raise RuntimeError("nats-py is required to run the DLQ monitor") from exc

        self.nc = NATS()
        await self.nc.connect(self.config.nats_url)
        self.js = self.nc.jetstream()
        self.sub = await self.js.pull_subscribe(
            subject=self.config.subject,
            durable=self.config.durable_name,
            stream=self.config.stream_name,
            config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW),
        )
        logger.info("DLQ monitor started for %s", self.config.subject)

        while True:
            try:
                messages = await self.sub.fetch(batch=self.config.batch_size, timeout=self.config.fetch_timeout_seconds)
                for message in messages:
                    await self._triage_message(message)
            except TimeoutError:
                continue

    async def _triage_message(self, message: Any) -> None:
        headers = dict(getattr(message, "headers", None) or {})
        with message_span(
            service_name="nexus.dlq.monitor",
            span_name="dlq.triage",
            incoming_headers=headers,
            attributes={
                "messaging.system": "nats",
                "messaging.destination": getattr(message, "subject", self.config.subject),
            },
        ):
            num_delivered = int(headers.get("Nats-Num-Delivered", "1"))
            last_error = headers.get("Nats-Last-Error", "unknown")
            source_subject = headers.get("Nats-Subject", getattr(message, "subject", "unknown"))
            diagnosis = self._classify_failure(last_error, num_delivered)

            alert = {
                "severity": diagnosis["severity"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source_subject": source_subject,
                "delivery_attempts": num_delivered,
                "last_error": last_error,
                "classification": diagnosis["type"],
                "recommended_action": diagnosis["action"],
                "payload_preview": self._payload_preview(getattr(message, "data", b"")),
            }

            logger.error("DLQ alert: %s", json.dumps(alert, indent=2))
            await self._send_alert(alert)
            await message.ack()

    def _classify_failure(self, error: str, attempts: int) -> dict[str, str]:
        error_lower = error.lower()
        if "deserialization" in error_lower or "schema" in error_lower:
            return {
                "severity": "CRITICAL",
                "type": "POISON_PILL_SCHEMA_VIOLATION",
                "action": "Inspect LLM output and update the schema or prompt before retrying.",
            }
        if ("timeout" in error_lower or "rate limit" in error_lower) and attempts >= 5:
            return {
                "severity": "HIGH",
                "type": "INFRASTRUCTURE_TIMEOUT_EXHAUSTED",
                "action": "Check upstream availability and consider a fallback provider.",
            }
        if "401" in error_lower or "403" in error_lower:
            return {
                "severity": "CRITICAL",
                "type": "AUTHENTICATION_FAILURE",
                "action": "Rotate credentials and verify secret delivery.",
            }
        return {
            "severity": "MEDIUM",
            "type": "UNKNOWN_FAILURE",
            "action": "Inspect the payload preview and broker metadata.",
        }

    @staticmethod
    def _payload_preview(payload: Any) -> str:
        if isinstance(payload, (bytes, bytearray)):
            return payload[:500].decode("utf-8", errors="replace")
        return str(payload)[:500]

    async def _send_alert(self, alert: dict[str, Any]) -> None:
        if not self.config.alert_webhook:
            return
        async with httpx.AsyncClient() as client:
            try:
                await client.post(self.config.alert_webhook, json=alert, timeout=5.0)
            except Exception as exc:  # pragma: no cover - network failure path
                logger.error("Failed to send DLQ alert: %s", exc)


async def main() -> None:
    monitor = DLQMonitor()
    await monitor.start()