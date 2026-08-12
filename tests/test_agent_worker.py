from __future__ import annotations

from datetime import datetime, timezone
import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nexus.agent_worker import (
    AgentWorker,
    AgentWorkerConfig,
    MockAgentModel,
    TaskAssignment,
    ToolExecutionDeniedError,
    ToolExecutionSandbox,
    ToolInvocation,
)


class FixedClock:
    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now


class FakeIdempotencyRepository:
    def __init__(self, claim_results: list[bool]) -> None:
        self.claim_results = list(claim_results)
        self.calls: list[tuple[str, str, str]] = []

    def claim(self, idempotency_key: str, execution_id: str, message_id: str) -> bool:
        self.calls.append((idempotency_key, execution_id, message_id))
        if self.claim_results:
            return self.claim_results.pop(0)
        return True


class FakePublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, object], dict[str, str] | None]] = []

    def publish(
        self,
        subject: str,
        message: dict[str, object],
        headers: dict[str, str] | None = None,
    ) -> None:
        self.published.append((subject, message, headers))


class CountingWebSearch:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def __call__(self, arguments: dict[str, object]) -> dict[str, object]:
        self.calls.append(dict(arguments))
        return {
            "query": arguments["query"],
            "endpoint": arguments["endpoint"],
            "results": [{"title": "result", "url": arguments["endpoint"]}],
        }


class AgentWorkerTests(unittest.TestCase):
    def _assignment(self) -> TaskAssignment:
        return TaskAssignment.from_mapping(
            {
                "message_id": "msg-1",
                "correlation_id": "exec-1",
                "parent_span_id": None,
                "sender_agent_id": "workflow-orchestrator",
                "recipient_agent_id": "researcher",
                "payload_schema_ref": "schemas/task-assignment.schema.json",
                "idempotency_key": "exec-1:planner",
                "timestamp": "2026-08-09T12:00:00+00:00",
                "message_type": "TASK_ASSIGNMENT",
                "payload": {
                    "execution_id": "exec-1",
                    "workflow_id": "research_pipeline",
                    "workflow_name": "Automated Research Pipeline",
                    "node_id": "planner",
                    "agent_role": "researcher",
                },
            }
        )

    def test_process_assignment_publishes_task_result_after_claim(self) -> None:
        web_search = CountingWebSearch()
        sandbox = ToolExecutionSandbox({"web_search": web_search})
        publisher = FakePublisher()
        worker = AgentWorker(
            idempotency_repository=FakeIdempotencyRepository([True]),
            tool_sandbox=sandbox,
            agent_model=MockAgentModel(),
            publisher=publisher,
            clock=FixedClock(datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)),
            config=AgentWorkerConfig(task_result_subject="nexus.task-result"),
            identifier_factory=lambda: "result-1",
        )

        published = worker.process_assignment(self._assignment())

        self.assertTrue(published)
        self.assertEqual(1, len(web_search.calls))
        self.assertEqual("Automated Research Pipeline planner", web_search.calls[0]["query"])
        self.assertEqual(1, len(publisher.published))
        subject, message, headers = publisher.published[0]
        self.assertEqual("nexus.task-result", subject)
        self.assertEqual("TASK_RESULT", message["message_type"])
        self.assertEqual("COMPLETED", message["payload"]["status"])
        self.assertEqual("web_search", message["payload"]["tool_name"])
        self.assertIsInstance(headers, dict)

    def test_duplicate_idempotency_key_skips_tool_and_publish(self) -> None:
        web_search = CountingWebSearch()
        sandbox = ToolExecutionSandbox({"web_search": web_search})
        publisher = FakePublisher()
        worker = AgentWorker(
            idempotency_repository=FakeIdempotencyRepository([False]),
            tool_sandbox=sandbox,
            agent_model=MockAgentModel(),
            publisher=publisher,
            clock=FixedClock(datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)),
            identifier_factory=lambda: "result-1",
        )

        published = worker.process_assignment(self._assignment())

        self.assertFalse(published)
        self.assertEqual([], web_search.calls)
        self.assertEqual([], publisher.published)

    def test_sandbox_rejects_shell_execution(self) -> None:
        sandbox = ToolExecutionSandbox({"web_search": CountingWebSearch()})

        with self.assertRaises(ToolExecutionDeniedError):
            sandbox.execute(ToolInvocation(name="shell", arguments={"command": "rm -rf /"}))

    def test_process_assignment_rejects_forbidden_tool_arguments(self) -> None:
        class ForbiddenToolModel:
            def plan_tool_invocation(self, assignment: TaskAssignment) -> ToolInvocation:
                return ToolInvocation(name="web_search", arguments={"command": "sh"})

            def compose_result(self, assignment: TaskAssignment, tool_result: dict[str, object]) -> dict[str, object]:
                return {"status": "UNUSED"}

        sandbox = ToolExecutionSandbox({"web_search": CountingWebSearch()})
        publisher = FakePublisher()
        worker = AgentWorker(
            idempotency_repository=FakeIdempotencyRepository([True]),
            tool_sandbox=sandbox,
            agent_model=ForbiddenToolModel(),
            publisher=publisher,
            clock=FixedClock(datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)),
            identifier_factory=lambda: "result-1",
        )

        with self.assertRaises(ToolExecutionDeniedError):
            worker.process_assignment(self._assignment())


if __name__ == "__main__":
    unittest.main()