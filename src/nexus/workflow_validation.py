from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


class WorkflowValidationError(ValueError):
    """Raised when a workflow graph is not safe to execute."""


@dataclass(frozen=True)
class WorkflowNode:
    node_id: str
    kind: str
    agent_role: str
    output_schema_ref: str


def validate_workflow_graph(workflow: Mapping[str, Any]) -> None:
    """Validate shape and safety rules for a workflow graph.

    The JSON Schema checks structure, while this function enforces graph-level
    invariants that cannot be expressed cleanly in JSON Schema alone.
    """

    nodes = _require_sequence(workflow, "nodes")
    edges = _require_sequence(workflow, "edges")
    entry_node_id = _require_string(workflow, "entry_node_id")
    terminal_node_ids = set(_require_sequence(workflow, "terminal_node_ids"))

    parsed_nodes = [_parse_node(node) for node in nodes]
    node_ids = [node.node_id for node in parsed_nodes]
    node_id_set = set(node_ids)

    _ensure_unique(node_ids, "node")
    _ensure_unique(list(terminal_node_ids), "terminal node")

    if entry_node_id not in node_id_set:
        raise WorkflowValidationError(f"entry_node_id '{entry_node_id}' does not exist in nodes")

    for terminal_node_id in terminal_node_ids:
        if terminal_node_id not in node_id_set:
            raise WorkflowValidationError(f"terminal node '{terminal_node_id}' does not exist in nodes")

    adjacency: dict[str, list[str]] = defaultdict(list)
    inbound_counts: dict[str, int] = {node_id: 0 for node_id in node_id_set}

    for edge in edges:
        if not isinstance(edge, Mapping):
            raise WorkflowValidationError("each edge must be an object")

        source = _require_string(edge, "from", "edge")
        target = _require_string(edge, "to", "edge")
        condition = _require_mapping(edge, "condition", "edge")
        condition_type = _require_string(condition, "type", "edge.condition")

        if source not in node_id_set:
            raise WorkflowValidationError(f"edge source '{source}' does not exist")
        if target not in node_id_set:
            raise WorkflowValidationError(f"edge target '{target}' does not exist")

        if condition_type == "expression" and not condition.get("expression"):
            raise WorkflowValidationError("expression edges must define an expression")
        if condition_type == "timeout" and "timeout_seconds" not in condition:
            raise WorkflowValidationError("timeout edges must define timeout_seconds")

        adjacency[source].append(target)
        inbound_counts[target] += 1

    cycle_path = _find_cycle(adjacency, entry_node_id)
    if cycle_path:
        cycle_text = " -> ".join(cycle_path + [cycle_path[0]])
        raise WorkflowValidationError(f"workflow contains a cycle: {cycle_text}")

    reachable_nodes = _collect_reachable_nodes(adjacency, entry_node_id)
    orphan_nodes = sorted(node_id for node_id in node_id_set if node_id not in reachable_nodes)
    if orphan_nodes:
        raise WorkflowValidationError(f"workflow contains unreachable nodes: {', '.join(orphan_nodes)}")

    for node in parsed_nodes:
        if node.kind == "terminal" and node.node_id not in terminal_node_ids:
            raise WorkflowValidationError(f"terminal node '{node.node_id}' must appear in terminal_node_ids")
        if node.node_id in terminal_node_ids and adjacency.get(node.node_id):
            raise WorkflowValidationError(f"terminal node '{node.node_id}' cannot have outgoing edges")

    if inbound_counts[entry_node_id] != 0:
        raise WorkflowValidationError("entry node must not have inbound edges")


def _parse_node(node: Any) -> WorkflowNode:
    if not isinstance(node, Mapping):
        raise WorkflowValidationError("each node must be an object")

    return WorkflowNode(
        node_id=_require_string(node, "id", "node"),
        kind=_require_string(node, "kind", "node"),
        agent_role=_require_string(node, "agent_role", "node"),
        output_schema_ref=_require_string(node, "output_schema_ref", "node"),
    )


def _require_sequence(obj: Mapping[str, Any], key: str) -> Sequence[Any]:
    value = obj.get(key)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise WorkflowValidationError(f"'{key}' must be an array")
    return value


def _require_mapping(obj: Mapping[str, Any], key: str, scope: str) -> Mapping[str, Any]:
    value = obj.get(key)
    if not isinstance(value, Mapping):
        raise WorkflowValidationError(f"'{scope}.{key}' must be an object")
    return value


def _require_string(obj: Mapping[str, Any], key: str, scope: str | None = None) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value.strip():
        location = f"{scope}.{key}" if scope else key
        raise WorkflowValidationError(f"'{location}' must be a non-empty string")
    return value


def _ensure_unique(values: Sequence[str], label: str) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    if duplicates:
        raise WorkflowValidationError(f"duplicate {label} identifiers: {', '.join(sorted(duplicates))}")


def _find_cycle(adjacency: Mapping[str, list[str]], entry_node_id: str) -> list[str]:
    visited: set[str] = set()
    active_path: list[str] = []
    active_set: set[str] = set()

    def visit(node_id: str) -> list[str] | None:
        visited.add(node_id)
        active_path.append(node_id)
        active_set.add(node_id)

        for neighbor in adjacency.get(node_id, []):
            if neighbor not in visited:
                cycle = visit(neighbor)
                if cycle:
                    return cycle
            elif neighbor in active_set:
                cycle_start = active_path.index(neighbor)
                return active_path[cycle_start:]

        active_path.pop()
        active_set.remove(node_id)
        return None

    return visit(entry_node_id) or []


def _collect_reachable_nodes(adjacency: Mapping[str, list[str]], entry_node_id: str) -> set[str]:
    reachable: set[str] = set()
    stack = [entry_node_id]
    while stack:
        node_id = stack.pop()
        if node_id in reachable:
            continue
        reachable.add(node_id)
        stack.extend(adjacency.get(node_id, []))
    return reachable

