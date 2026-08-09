from __future__ import annotations

from datetime import datetime, timezone
import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nexus import WorkflowExecutionState
from nexus.orchestrator import (
    LeaseToken,
    LeaseUnavailableError,
    OrchestratorConfig,
    WorkflowDefinition,
    WorkflowExecutionRecord,
    WorkflowOrchestrator,
)


class FixedClock:
    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now


class FakeDefinitionRepository:
    def __init__(self, definition: WorkflowDefinition) -> None:
        self.definition = definition
        self.loaded_ids: list[str] = []

    def load(self, workflow_id: str) -> WorkflowDefinition:
        self.loaded_ids.append(workflow_id)
        return self.definition


class FakeExecutionRepository:
    def __init__(self) -> None:
        self.created: list[WorkflowExecutionRecord] = []
        self.transitions: list[tuple[str, str, datetime, datetime, int]] = []

    def create(self, execution: WorkflowExecutionRecord) -> WorkflowExecutionRecord:
        self.created.append(execution)
        return execution

    def transition_pending_to_running(
        self,
        execution_id: str,
        lease_owner: str,
        lease_expires_at: datetime,
        heartbeat_at: datetime,
        expected_version: int,
    ) -> WorkflowExecutionRecord:
        self.transitions.append((execution_id, lease_owner, lease_expires_at, heartbeat_at, expected_version))
        created = self.created[-1]
        return WorkflowExecutionRecord(
            execution_id=created.execution_id,
            workflow_id=created.workflow_id,
            state=WorkflowExecutionState.RUNNING,
            current_node_id=created.current_node_id,
            last_committed_node_id=created.last_committed_node_id,
            version=created.version + 1,
            lease_owner=lease_owner,
            lease_expires_at=lease_expires_at,
            heartbeat_at=heartbeat_at,
            resume_token=created.resume_token,
            human_input_required=created.human_input_required,
            failure_reason=created.failure_reason,
            created_at=created.created_at,
            updated_at=heartbeat_at,
        )


class FakeLeaseManager:
    def __init__(self, lease: LeaseToken | None) -> None:
        self.lease = lease
        self.acquired: list[tuple[str, str, int]] = []

    def acquire(self, lease_key: str, owner_id: str, ttl_seconds: int) -> LeaseToken | None:
        self.acquired.append((lease_key, owner_id, ttl_seconds))
        return self.lease


class FakePublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []

    def publish(self, subject: str, message: dict[str, object]) -> None:
        self.published.append((subject, message))


class OrchestratorTests(unittest.TestCase):
    def test_start_workflow_acquires_lease_and_publishes_assignment(self) -> None:
        definition = WorkflowDefinition.from_mapping(
            {
                "workflow_id": "research_pipeline",
                "name": "Automated Research Pipeline",
                "entry_node_id": "planner",
                "nodes": [
                    {"id": "planner", "kind": "agent", "agent_role": "planner", "output_schema_ref": "planner.json"},
                    {"id": "done", "kind": "terminal", "agent_role": "terminal", "output_schema_ref": "done.json"},
                ],
                "edges": [{"from": "planner", "to": "done", "condition": {"type": "always"}}],
                "terminal_node_ids": ["done"],
            }
        )
        clock = FixedClock(datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc))
        definition_repository = FakeDefinitionRepository(definition)
        execution_repository = FakeExecutionRepository()
        lease_manager = FakeLeaseManager(
            LeaseToken(
                lease_key="workflow-execution:nexus-00000001",
                owner_id="workflow-orchestrator",
                expires_at=datetime(2026, 8, 8, 12, 1, tzinfo=timezone.utc),
            )
        )
        publisher = FakePublisher()
        orchestrator = WorkflowOrchestrator(
            definition_repository=definition_repository,
            execution_repository=execution_repository,
            lease_manager=lease_manager,
            publisher=publisher,
            clock=clock,
            config=OrchestratorConfig(lease_ttl_seconds=60),
            identifier_factory=lambda: "nexus-00000001",
        )

        execution = orchestrator.start_workflow("research_pipeline")

        self.assertEqual(["research_pipeline"], definition_repository.loaded_ids)
        self.assertEqual([("workflow-execution:nexus-00000001", "workflow-orchestrator", 60)], lease_manager.acquired)
        self.assertEqual(1, len(execution_repository.created))
        self.assertEqual(1, len(execution_repository.transitions))
        self.assertEqual(1, len(publisher.published))
        self.assertEqual(WorkflowExecutionState.RUNNING, execution.state)
        self.assertEqual("planner", execution.current_node_id)
        self.assertEqual("nexus.task-assignment", publisher.published[0][0])
        self.assertEqual("TASK_ASSIGNMENT", publisher.published[0][1]["message_type"])
        self.assertEqual("planner", publisher.published[0][1]["payload"]["agent_role"])

    def test_start_workflow_rejects_missing_lease(self) -> None:
        definition = WorkflowDefinition.from_mapping(
            {
                "workflow_id": "research_pipeline",
                "name": "Automated Research Pipeline",
                "entry_node_id": "planner",
                "nodes": [
                    {"id": "planner", "kind": "agent", "agent_role": "planner", "output_schema_ref": "planner.json"},
                    {"id": "done", "kind": "terminal", "agent_role": "terminal", "output_schema_ref": "done.json"},
                ],
                "edges": [{"from": "planner", "to": "done", "condition": {"type": "always"}}],
                "terminal_node_ids": ["done"],
            }
        )
        orchestrator = WorkflowOrchestrator(
            definition_repository=FakeDefinitionRepository(definition),
            execution_repository=FakeExecutionRepository(),
            lease_manager=FakeLeaseManager(None),
            publisher=FakePublisher(),
            clock=FixedClock(datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)),
            identifier_factory=lambda: "nexus-00000001",
        )

        with self.assertRaises(LeaseUnavailableError):
            orchestrator.start_workflow("research_pipeline")


if __name__ == "__main__":
    unittest.main()