from __future__ import annotations

import ipaddress
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Protocol
from urllib.parse import urlparse
from uuid import uuid4

from .otel_setup import TelemetryConfig, configure_otel, inject_trace_headers, message_span


@dataclass(frozen=True)
class AgentWorkerConfig:
    worker_agent_id: str = "research-agent-worker"
    orchestrator_agent_id: str = "workflow-orchestrator"
    task_assignment_subject: str = "nexus.task-assignment"
    task_result_subject: str = "nexus.task-result"
    task_result_payload_schema_ref: str = "schemas/task-result.schema.json"
    consumer_durable_name: str = "research-agent-worker"
    stream_name: str = "nexus-tasks"
    max_batch_size: int = 10
    fetch_timeout_seconds: float = 1.0
    tool_name: str = "web_search"


@dataclass(frozen=True)
class ToolInvocation:
    name: str
    arguments: Mapping[str, Any]


@dataclass(frozen=True)
class ToolPolicy:
    """
    Política de seguridad para ejecución de herramientas.
    Usa listas BLANCAS (allowlists) por defecto. Default-deny para todo lo no explícitamente permitido.
    """
    # Argumentos permitidos por herramienta (default-deny: cualquier otro argumento es rechazado)
    allowed_arguments_per_tool: Mapping[str, frozenset[str]] = field(default_factory=lambda: {
        "web_search": frozenset({"query", "endpoint", "max_results", "language"}),
        "db_query": frozenset({"sql", "params", "timeout_seconds"}),
        "file_read": frozenset({"path", "encoding", "max_bytes"}),
    })
    
    # Hosts de red permitidos (dominios exactos)
    allowed_network_hosts: frozenset[str] = frozenset({
        "example.com",
        "www.example.com",
        "api.openai.com",
        "search.brave.com",
    })
    
    # Esquemas de URL permitidos (HTTP excluido en producción; solo HTTPS)
    allowed_schemes: frozenset[str] = frozenset({"https"})


@dataclass(frozen=True)
class TaskAssignment:
    message_id: str
    correlation_id: str
    parent_span_id: str | None
    sender_agent_id: str
    recipient_agent_id: str
    payload_schema_ref: str
    idempotency_key: str
    timestamp: datetime
    message_type: str
    payload: Mapping[str, Any]

    @classmethod
    def from_mapping(cls, message: Mapping[str, Any]) -> "TaskAssignment":
        if str(message.get("message_type")) != "TASK_ASSIGNMENT":
            raise ValueError("expected a TASK_ASSIGNMENT message")

        timestamp_value = message.get("timestamp")
        if not isinstance(timestamp_value, str) or not timestamp_value.strip():
            raise ValueError("'timestamp' must be a non-empty string")

        payload = message.get("payload")
        if not isinstance(payload, Mapping):
            raise ValueError("'payload' must be an object")

        return cls(
            message_id=_require_string(message, "message_id"),
            correlation_id=_require_string(message, "correlation_id"),
            parent_span_id=_optional_string(message, "parent_span_id"),
            sender_agent_id=_require_string(message, "sender_agent_id"),
            recipient_agent_id=_require_string(message, "recipient_agent_id"),
            payload_schema_ref=_require_string(message, "payload_schema_ref"),
            idempotency_key=_require_string(message, "idempotency_key"),
            timestamp=_parse_timestamp(timestamp_value),
            message_type=_require_string(message, "message_type"),
            payload=payload,
        )

    @property
    def execution_id(self) -> str:
        return _require_string(self.payload, "execution_id")

    @property
    def workflow_id(self) -> str:
        return _require_string(self.payload, "workflow_id")

    @property
    def workflow_name(self) -> str:
        return _require_string(self.payload, "workflow_name")

    @property
    def node_id(self) -> str:
        return _require_string(self.payload, "node_id")

    @property
    def agent_role(self) -> str:
        return _require_string(self.payload, "agent_role")


@dataclass(frozen=True)
class TaskResultMessage:
    message_id: str
    correlation_id: str
    parent_span_id: str | None
    sender_agent_id: str
    recipient_agent_id: str
    payload_schema_ref: str
    idempotency_key: str
    timestamp: datetime
    message_type: str
    payload: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "correlation_id": self.correlation_id,
            "parent_span_id": self.parent_span_id,
            "sender_agent_id": self.sender_agent_id,
            "recipient_agent_id": self.recipient_agent_id,
            "payload_schema_ref": self.payload_schema_ref,
            "idempotency_key": self.idempotency_key,
            "timestamp": self.timestamp.isoformat(),
            "message_type": self.message_type,
            "payload": dict(self.payload),
        }


class IdempotencyRepository(Protocol):
    def claim(self, idempotency_key: str, execution_id: str, message_id: str) -> bool:
        raise NotImplementedError


class MessagePublisher(Protocol):
    def publish(
        self,
        subject: str,
        message: Mapping[str, Any],
        headers: Mapping[str, str] | None = None,
    ) -> None:
        raise NotImplementedError


class AgentModel(Protocol):
    def plan_tool_invocation(self, assignment: TaskAssignment) -> ToolInvocation:
        raise NotImplementedError

    def compose_result(self, assignment: TaskAssignment, tool_result: Mapping[str, Any]) -> Mapping[str, Any]:
        raise NotImplementedError


class ToolHandler(Protocol):
    def __call__(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        raise NotImplementedError


class Clock(Protocol):
    def now(self) -> datetime:
        raise NotImplementedError


class ToolExecutionDeniedError(RuntimeError):
    pass


class ToolExecutionSandbox:
    """
    Sandbox de ejecución de herramientas con validación estricta por lista blanca.
    
    Principios de seguridad:
    1. Default-deny: todo argumento no explícitamente permitido es rechazado.
    2. Validación de esquema de URL (solo HTTPS en producción).
    3. Validación de hostname contra allowlist (previene SSRF).
    4. Detección de IPs privadas/loopback (defensa en profundidad).
    5. No hay ejecución de código arbitrario (comandos shell, scripts).
    """
    
    def __init__(
        self, 
        tool_registry: Mapping[str, ToolHandler], 
        policy: ToolPolicy | None = None
    ) -> None:
        self.tool_registry = dict(tool_registry)
        self.policy = policy or ToolPolicy()

    def execute(self, invocation: ToolInvocation) -> Mapping[str, Any]:
        # 1. Verificar que la herramienta está en el registro permitido
        if invocation.name not in self.policy.allowed_arguments_per_tool:
            raise ToolExecutionDeniedError(
                f"tool '{invocation.name}' is not in the allowed tools registry. "
                f"Allowed: {sorted(self.policy.allowed_arguments_per_tool.keys())}"
            )
        
        handler = self.tool_registry.get(invocation.name)
        if handler is None:
            raise ToolExecutionDeniedError(
                f"tool '{invocation.name}' has no registered handler"
            )

        # 2. Validación estricta de argumentos (lista blanca)
        self._validate_arguments(invocation)
        
        # 3. Ejecución dentro del sandbox
        return handler(invocation.arguments)

    def _validate_arguments(self, invocation: ToolInvocation) -> None:
        allowed_args = self.policy.allowed_arguments_per_tool[invocation.name]
        provided_args = set(invocation.arguments.keys())
        
        # Detección de argumentos no permitidos (default-deny)
        forbidden_args = provided_args - allowed_args
        if forbidden_args:
            raise ToolExecutionDeniedError(
                f"tool '{invocation.name}' received forbidden arguments: "
                f"{sorted(forbidden_args)}. Allowed: {sorted(allowed_args)}"
            )
        
        # Validación específica para endpoints de red (previene SSRF)
        endpoint = invocation.arguments.get("endpoint") or invocation.arguments.get("url")
        if endpoint is not None:
            self._validate_network_endpoint(str(endpoint), invocation.name)

    def _validate_network_endpoint(self, endpoint: str, tool_name: str) -> None:
        parsed = urlparse(endpoint)
        
        # Rechazar esquemas no permitidos (previene file://, javascript:, data:, etc.)
        if parsed.scheme not in self.policy.allowed_schemes:
            raise ToolExecutionDeniedError(
                f"tool '{tool_name}': scheme '{parsed.scheme}' is not allowed. "
                f"Allowed: {sorted(self.policy.allowed_schemes)}"
            )
        
        # Rechazar endpoints sin hostname válido
        if not parsed.hostname:
            raise ToolExecutionDeniedError(
                f"tool '{tool_name}': endpoint must have a valid hostname"
            )
        
        # Defensa en profundidad: rechazar IPs privadas, loopback y link-local
        # Esto bloquea ataques SSRF incluso si el atacante controla DNS
        if self._is_private_ip(parsed.hostname):
            raise ToolExecutionDeniedError(
                f"tool '{tool_name}': private/loopback/link-local IPs are forbidden"
            )
        
        # Verificación de hostname contra allowlist
        if parsed.hostname not in self.policy.allowed_network_hosts:
            raise ToolExecutionDeniedError(
                f"tool '{tool_name}': hostname '{parsed.hostname}' is not in allowlist. "
                f"Allowed: {sorted(self.policy.allowed_network_hosts)}"
            )

    @staticmethod
    def _is_private_ip(hostname: str) -> bool:
        """
        Detecta IPs privadas, loopback, link-local y reservadas para prevenir SSRF.
        Retorna False si el hostname no es una IP (es un dominio).
        """
        try:
            ip = ipaddress.ip_address(hostname)
            return (
                ip.is_private
                or ip.is_loopback
                or ip.is_link_local
                or ip.is_reserved
                or ip.is_multicast
            )
        except ValueError:
            # No es una IP literal, es un dominio. La allowlist de hostnames lo cubre.
            return False


class MockAgentModel:
    def plan_tool_invocation(self, assignment: TaskAssignment) -> ToolInvocation:
        query = f"{assignment.workflow_name} {assignment.node_id}"
        return ToolInvocation(
            name="web_search",
            arguments={
                "query": query,
                "endpoint": "https://example.com/search",
            },
        )

    def compose_result(self, assignment: TaskAssignment, tool_result: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            "execution_id": assignment.execution_id,
            "workflow_id": assignment.workflow_id,
            "node_id": assignment.node_id,
            "agent_role": assignment.agent_role,
            "status": "COMPLETED",
            "summary": f"Mock completion for {assignment.workflow_name}",
            "tool_result": dict(tool_result),
        }


class AgentWorker:
    def __init__(
        self,
        idempotency_repository: IdempotencyRepository,
        tool_sandbox: ToolExecutionSandbox,
        agent_model: AgentModel,
        publisher: MessagePublisher,
        clock: Clock,
        config: AgentWorkerConfig | None = None,
        identifier_factory: Callable[[], str] | None = None,
    ) -> None:
        self.idempotency_repository = idempotency_repository
        self.tool_sandbox = tool_sandbox
        self.agent_model = agent_model
        self.publisher = publisher
        self.clock = clock
        self.config = config or AgentWorkerConfig()
        self.identifier_factory = identifier_factory or (lambda: uuid4().hex)
        configure_otel(TelemetryConfig(service_name="nexus.agent.researcher"))

    async def run(self, jetstream: Any) -> None:
        consumer = await jetstream.pull_subscribe(
            self.config.task_assignment_subject,
            durable=self.config.consumer_durable_name,
            stream=self.config.stream_name,
        )

        while True:
            messages = await consumer.fetch(
                batch=self.config.max_batch_size,
                timeout=self.config.fetch_timeout_seconds,
            )
            for message in messages:
                await self._handle_message(message)

    async def _handle_message(self, message: Any) -> None:
        raw_payload = message.data.decode("utf-8") if isinstance(message.data, (bytes, bytearray)) else message.data
        headers = dict(getattr(message, "headers", None) or {})
        try:
            assignment = TaskAssignment.from_mapping(json.loads(raw_payload))
        except (json.JSONDecodeError, ValueError):
            await message.term()
            return

        try:
            published = self.process_assignment(assignment, trace_headers=headers)
        except ToolExecutionDeniedError:
            await message.term()
            return
        except Exception:
            attempts = self._delivery_attempts(headers)
            await message.nak(delay=self._retry_delay_seconds(attempts))
            return

        if published:
            await message.ack()
        else:
            await message.ack()

    def process_assignment(self, assignment: TaskAssignment, trace_headers: Mapping[str, str] | None = None) -> bool:
        with message_span(
            service_name="nexus.agent.researcher",
            span_name="agent.process_task",
            incoming_headers=trace_headers,
            attributes={
                "messaging.system": "nats",
                "messaging.destination": self.config.task_assignment_subject,
                "messaging.operation": "process",
                "workflow.execution_id": assignment.execution_id,
            },
        ):
            with message_span(service_name="nexus.agent.researcher", span_name="agent.idempotency_check"):
                claimed = self.idempotency_repository.claim(
                    assignment.idempotency_key,
                    assignment.execution_id,
                    assignment.message_id,
                )
            if not claimed:
                return False

            with message_span(service_name="nexus.agent.researcher", span_name="agent.tool_selection"):
                tool_invocation = self.agent_model.plan_tool_invocation(assignment)

            with message_span(service_name="nexus.agent.researcher", span_name="agent.tool_execution"):
                tool_result = self.tool_sandbox.execute(tool_invocation)

            with message_span(service_name="nexus.agent.researcher", span_name="agent.compose_result"):
                model_result = self.agent_model.compose_result(assignment, tool_result)

            result_message = TaskResultMessage(
                message_id=self.identifier_factory(),
                correlation_id=assignment.correlation_id,
                parent_span_id=assignment.message_id,
                sender_agent_id=self.config.worker_agent_id,
                recipient_agent_id=self.config.orchestrator_agent_id,
                payload_schema_ref=self.config.task_result_payload_schema_ref,
                idempotency_key=assignment.idempotency_key,
                timestamp=self.clock.now(),
                message_type="TASK_RESULT",
                payload={
                    **dict(model_result),
                    "tool_name": tool_invocation.name,
                    "tool_arguments": dict(tool_invocation.arguments),
                },
            )
            self.publisher.publish(self.config.task_result_subject, result_message.to_dict(), headers=inject_trace_headers())
            return True

    @staticmethod
    def _delivery_attempts(headers: Mapping[str, Any]) -> int:
        value = headers.get("Nats-Num-Delivered", "1")
        try:
            return max(1, int(str(value)))
        except (TypeError, ValueError):
            return 1

    @staticmethod
    def _retry_delay_seconds(attempts: int) -> int:
        return min(120, 5 * (2 ** max(0, attempts - 1)))


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


def build_default_tool_registry() -> dict[str, ToolHandler]:
    def web_search(arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        query = _require_string(arguments, "query")
        endpoint = str(arguments.get("endpoint", "https://example.com/search"))
        return {
            "query": query,
            "endpoint": endpoint,
            "results": [
                {
                    "title": f"Mock result for {query}",
                    "url": endpoint,
                    "snippet": f"Mocked web-search response for '{query}'",
                }
            ],
        }

    return {"web_search": web_search}


def create_agent_worker(
    idempotency_repository: IdempotencyRepository,
    publisher: MessagePublisher,
    config: AgentWorkerConfig | None = None,
) -> AgentWorker:
    return AgentWorker(
        idempotency_repository=idempotency_repository,
        tool_sandbox=ToolExecutionSandbox(build_default_tool_registry()),
        agent_model=MockAgentModel(),
        publisher=publisher,
        clock=SystemClock(),
        config=config,
        identifier_factory=lambda: uuid4().hex,
    )


def _require_string(obj: Mapping[str, Any], key: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"'{key}' must be a non-empty string")
    return value


def _optional_string(obj: Mapping[str, Any], key: str) -> str | None:
    value = obj.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"'{key}' must be a non-empty string when present")
    return value


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed
