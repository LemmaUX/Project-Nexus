from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Protocol
from urllib.parse import urlparse
from uuid import uuid4


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
    allowed_tool_names: frozenset[str] = frozenset({"web_search"})
    allowed_network_hosts: frozenset[str] = frozenset({"example.com", "www.example.com"})


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
    def publish(self, subject: str, message: Mapping[str, Any]) -> None:
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
    def __init__(self, tool_registry: Mapping[str, ToolHandler], policy: ToolPolicy | None = None) -> None:
        self.tool_registry = dict(tool_registry)
        self.policy = policy or ToolPolicy()

    def execute(self, invocation: ToolInvocation) -> Mapping[str, Any]:
        if invocation.name not in self.policy.allowed_tool_names:
            raise ToolExecutionDeniedError(f"tool '{invocation.name}' is not enabled")

        handler = self.tool_registry.get(invocation.name)
        if handler is None:
            raise ToolExecutionDeniedError(f"tool '{invocation.name}' is not registered")

        self._validate_arguments(invocation)
        return handler(invocation.arguments)

    def _validate_arguments(self, invocation: ToolInvocation) -> None:
        for key in ("command", "argv", "shell", "executable", "script"):
            if key in invocation.arguments:
                raise ToolExecutionDeniedError(f"tool '{invocation.name}' attempted a forbidden argument: {key}")

        endpoint = invocation.arguments.get("endpoint") or invocation.arguments.get("url")
        if endpoint is None:
            return

        parsed = urlparse(str(endpoint))
        if parsed.scheme not in {"https", "http"}:
            raise ToolExecutionDeniedError("tool endpoints must use http or https")
        if not parsed.hostname or parsed.hostname not in self.policy.allowed_network_hosts:
            raise ToolExecutionDeniedError(f"network endpoint '{endpoint}' is not allowed")


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
        try:
            assignment = TaskAssignment.from_mapping(json.loads(raw_payload))
        except (json.JSONDecodeError, ValueError):
            await message.ack()
            return

        try:
            published = self.process_assignment(assignment)
        except Exception:
            await message.nak()
            return

        if published:
            await message.ack()
        else:
            await message.ack()

    def process_assignment(self, assignment: TaskAssignment) -> bool:
        claimed = self.idempotency_repository.claim(
            assignment.idempotency_key,
            assignment.execution_id,
            assignment.message_id,
        )
        if not claimed:
            return False

        tool_invocation = self.agent_model.plan_tool_invocation(assignment)
        tool_result = self.tool_sandbox.execute(tool_invocation)
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
        self.publisher.publish(self.config.task_result_subject, result_message.to_dict())
        return True


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