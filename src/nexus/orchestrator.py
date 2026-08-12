from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Protocol
from uuid import uuid4

from .otel_setup import TelemetryConfig, configure_otel, inject_trace_headers, message_span
from .state_machine import WorkflowExecutionState
from .workflow_validation import validate_workflow_graph


@dataclass(frozen=True)
class WorkflowDefinition:
    workflow_id: str
    name: str
    entry_node_id: str
    nodes: tuple[Mapping[str, Any], ...]
    edges: tuple[Mapping[str, Any], ...]
    terminal_node_ids: tuple[str, ...]
    description: str | None = None

    @classmethod
    def from_mapping(cls, workflow: Mapping[str, Any]) -> "WorkflowDefinition":
        return cls(
            workflow_id=str(workflow["workflow_id"]),
            name=str(workflow["name"]),
            entry_node_id=str(workflow["entry_node_id"]),
            nodes=tuple(workflow["nodes"]),
            edges=tuple(workflow["edges"]),
            terminal_node_ids=tuple(str(node_id) for node_id in workflow["terminal_node_ids"]),
            description=workflow.get("description"),
        )

    def to_validation_payload(self) -> dict[str, Any]:
        payload = {
            "workflow_id": self.workflow_id,
            "name": self.name,
            "entry_node_id": self.entry_node_id,
            "nodes": list(self.nodes),
            "edges": list(self.edges),
            "terminal_node_ids": list(self.terminal_node_ids),
        }
        if self.description is not None:
            payload["description"] = self.description
        return payload


@dataclass(frozen=True)
class WorkflowExecutionRecord:
    execution_id: str
    workflow_id: str
    state: WorkflowExecutionState
    current_node_id: str | None
    last_committed_node_id: str | None
    version: int
    lease_owner: str | None
    lease_expires_at: datetime | None
    heartbeat_at: datetime | None
    resume_token: str | None
    human_input_required: bool
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class LeaseToken:
    lease_key: str
    owner_id: str
    expires_at: datetime


@dataclass(frozen=True)
class TaskAssignmentMessage:
    message_id: str
    correlation_id: str
    parent_span_id: str | None
    sender_agent_id: str
    recipient_agent_id: str
    payload_schema_ref: str
    idempotency_key: str
    timestamp: datetime
    message_type: str
    payload: dict[str, Any]

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
            "payload": self.payload,
        }


class WorkflowDefinitionRepository(Protocol):
    def load(self, workflow_id: str) -> WorkflowDefinition:
        raise NotImplementedError


class WorkflowExecutionRepository(Protocol):
    def create(self, execution: WorkflowExecutionRecord) -> WorkflowExecutionRecord:
        raise NotImplementedError

    def transition_pending_to_running(
        self,
        execution_id: str,
        lease_owner: str,
        lease_expires_at: datetime,
        heartbeat_at: datetime,
        expected_version: int,
    ) -> WorkflowExecutionRecord:
        raise NotImplementedError


class LeaseManager(Protocol):
    def acquire(self, lease_key: str, owner_id: str, ttl_seconds: int) -> LeaseToken | None:
        raise NotImplementedError


class MessagePublisher(Protocol):
    def publish(
        self,
        subject: str,
        message: Mapping[str, Any],
        headers: Mapping[str, str] | None = None,
    ) -> None:
        raise NotImplementedError


class Clock(Protocol):
    def now(self) -> datetime:
        raise NotImplementedError


@dataclass(frozen=True)
class OrchestratorConfig:
    lease_ttl_seconds: int = 30
    orchestrator_agent_id: str = "workflow-orchestrator"
    task_assignment_subject: str = "nexus.task-assignment"
    task_assignment_payload_schema_ref: str = "schemas/task-assignment.schema.json"


class LeaseUnavailableError(RuntimeError):
    pass


class WorkflowOrchestrator:
    def __init__(
        self,
        definition_repository: WorkflowDefinitionRepository,
        execution_repository: WorkflowExecutionRepository,
        lease_manager: LeaseManager,
        publisher: MessagePublisher,
        clock: Clock,
        config: OrchestratorConfig | None = None,
        identifier_factory: Callable[[], str] | None = None,
    ) -> None:
        self.definition_repository = definition_repository
        self.execution_repository = execution_repository
        self.lease_manager = lease_manager
        self.publisher = publisher
        self.clock = clock
        self.config = config or OrchestratorConfig()
        self.identifier_factory = identifier_factory or (lambda: uuid4().hex)
        configure_otel(TelemetryConfig(service_name="nexus.orchestrator"))

    def start_workflow(self, workflow_id: str) -> WorkflowExecutionRecord:
        with message_span(
            service_name="nexus.orchestrator",
            span_name="orchestrator.start_workflow",
            attributes={"workflow.id": workflow_id},
        ):
            workflow_definition = self.definition_repository.load(workflow_id)
            validate_workflow_graph(workflow_definition.to_validation_payload())

            started_at = self.clock.now()
            pending_execution = WorkflowExecutionRecord(
                execution_id=self.identifier_factory(),
                workflow_id=workflow_definition.workflow_id,
                state=WorkflowExecutionState.PENDING,
                current_node_id=workflow_definition.entry_node_id,
                last_committed_node_id=None,
                version=0,
                lease_owner=None,
                lease_expires_at=None,
                heartbeat_at=None,
                resume_token=None,
                human_input_required=False,
                failure_reason=None,
                created_at=started_at,
                updated_at=started_at,
            )
            execution = self.execution_repository.create(pending_execution)

            lease_key = f"workflow-execution:{execution.execution_id}"
            lease = self.lease_manager.acquire(lease_key, self.config.orchestrator_agent_id, self.config.lease_ttl_seconds)
            if lease is None:
                raise LeaseUnavailableError(f"unable to acquire lease for {lease_key}")

            running_execution = self.execution_repository.transition_pending_to_running(
                execution_id=execution.execution_id,
                lease_owner=lease.owner_id,
                lease_expires_at=lease.expires_at,
                heartbeat_at=self.clock.now(),
                expected_version=execution.version,
            )

            entry_node = self._require_entry_node(workflow_definition)
            message = TaskAssignmentMessage(
                message_id=self.identifier_factory(),
                correlation_id=execution.execution_id,
                parent_span_id=None,
                sender_agent_id=self.config.orchestrator_agent_id,
                recipient_agent_id=str(entry_node["agent_role"]),
                payload_schema_ref=self.config.task_assignment_payload_schema_ref,
                idempotency_key=f"{execution.execution_id}:{workflow_definition.entry_node_id}",
                timestamp=self.clock.now(),
                message_type="TASK_ASSIGNMENT",
                payload={
                    "execution_id": execution.execution_id,
                    "workflow_id": workflow_definition.workflow_id,
                    "workflow_name": workflow_definition.name,
                    "node_id": workflow_definition.entry_node_id,
                    "agent_role": entry_node["agent_role"],
                    "lease_owner": lease.owner_id,
                    "lease_expires_at": lease.expires_at.isoformat(),
                },
            )
            self.publisher.publish(self.config.task_assignment_subject, message.to_dict(), headers=inject_trace_headers())
            return running_execution

    @staticmethod
    def _require_entry_node(workflow_definition: WorkflowDefinition) -> Mapping[str, Any]:
        for node in workflow_definition.nodes:
            if str(node.get("id")) == workflow_definition.entry_node_id:
                return node
        raise ValueError(f"entry node '{workflow_definition.entry_node_id}' does not exist")


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class SequentialIdentifierFactory:
    def __init__(self) -> None:
        self._counter = 0

    def __call__(self) -> str:
        self._counter += 1
        return f"nexus-{self._counter:08d}"


def create_orchestrator(
    definition_repository: WorkflowDefinitionRepository,
    execution_repository: WorkflowExecutionRepository,
    lease_manager: LeaseManager,
    publisher: MessagePublisher,
    config: OrchestratorConfig | None = None,
) -> WorkflowOrchestrator:
    return WorkflowOrchestrator(
        definition_repository=definition_repository,
        execution_repository=execution_repository,
        lease_manager=lease_manager,
        publisher=publisher,
        clock=SystemClock(),
        config=config,
        identifier_factory=SequentialIdentifierFactory(),
    )