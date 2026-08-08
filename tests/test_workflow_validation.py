from __future__ import annotations

import json
import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nexus import WorkflowExecutionState, is_valid_transition, validate_workflow_graph
from nexus.workflow_validation import WorkflowValidationError


class WorkflowValidationTests(unittest.TestCase):
    def test_valid_graph_passes(self) -> None:
        graph = {
            "workflow_id": "research_pipeline",
            "name": "Automated Research Pipeline",
            "entry_node_id": "planner",
            "nodes": [
                {
                    "id": "planner",
                    "kind": "agent",
                    "agent_role": "planner",
                    "output_schema_ref": "schemas/planner-output.schema.json",
                },
                {
                    "id": "researcher",
                    "kind": "agent",
                    "agent_role": "researcher",
                    "output_schema_ref": "schemas/researcher-output.schema.json",
                },
                {
                    "id": "writer",
                    "kind": "agent",
                    "agent_role": "writer",
                    "output_schema_ref": "schemas/writer-output.schema.json",
                },
                {
                    "id": "done",
                    "kind": "terminal",
                    "agent_role": "terminal",
                    "output_schema_ref": "schemas/terminal-output.schema.json",
                },
            ],
            "edges": [
                {"from": "planner", "to": "researcher", "condition": {"type": "always"}},
                {"from": "researcher", "to": "writer", "condition": {"type": "always"}},
                {"from": "writer", "to": "done", "condition": {"type": "always"}},
            ],
            "terminal_node_ids": ["done"],
        }

        validate_workflow_graph(graph)

    def test_cycle_is_rejected(self) -> None:
        graph = {
            "workflow_id": "cyclic",
            "name": "Cyclic Graph",
            "entry_node_id": "a",
            "nodes": [
                {"id": "a", "kind": "agent", "agent_role": "a", "output_schema_ref": "a.json"},
                {"id": "b", "kind": "agent", "agent_role": "b", "output_schema_ref": "b.json"},
                {"id": "done", "kind": "terminal", "agent_role": "terminal", "output_schema_ref": "done.json"},
            ],
            "edges": [
                {"from": "a", "to": "b", "condition": {"type": "always"}},
                {"from": "b", "to": "a", "condition": {"type": "always"}},
                {"from": "a", "to": "done", "condition": {"type": "always"}},
            ],
            "terminal_node_ids": ["done"],
        }

        with self.assertRaises(WorkflowValidationError):
            validate_workflow_graph(graph)

    def test_orphan_node_is_rejected(self) -> None:
        graph = {
            "workflow_id": "orphan",
            "name": "Orphan Graph",
            "entry_node_id": "a",
            "nodes": [
                {"id": "a", "kind": "agent", "agent_role": "a", "output_schema_ref": "a.json"},
                {"id": "b", "kind": "agent", "agent_role": "b", "output_schema_ref": "b.json"},
            ],
            "edges": [],
            "terminal_node_ids": ["b"],
        }

        with self.assertRaises(WorkflowValidationError):
            validate_workflow_graph(graph)


class WorkflowStateMachineTests(unittest.TestCase):
    def test_valid_transition_matrix(self) -> None:
        self.assertTrue(is_valid_transition(WorkflowExecutionState.PENDING, WorkflowExecutionState.RUNNING))
        self.assertTrue(is_valid_transition(WorkflowExecutionState.RUNNING, WorkflowExecutionState.COMPLETED))
        self.assertFalse(is_valid_transition(WorkflowExecutionState.COMPLETED, WorkflowExecutionState.RUNNING))


class SchemaFileTests(unittest.TestCase):
    def test_schema_files_are_valid_json(self) -> None:
        for relative_path in [
            "schemas/workflow-graph.schema.json",
            "schemas/workflow-execution.schema.json",
            "schemas/inter-agent-message.schema.json",
        ]:
            with self.subTest(relative_path=relative_path):
                with (ROOT / relative_path).open("r", encoding="utf-8") as handle:
                    json.load(handle)


if __name__ == "__main__":
    unittest.main()
