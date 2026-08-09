# ADR 002: Language Selection for Orchestrator

## Status

Accepted.

## Context

Day 2 adds the workflow orchestrator: it must load workflow definitions from PostgreSQL, validate graph safety, create executions, acquire Redis leases, and publish the first `TASK_ASSIGNMENT` to NATS JetStream.

The repository is already Python-first. The workflow validator, execution state machine, JSON-schema contract tests, and the current package layout all live in `src/nexus/` and `tests/` as Python code.

## Decision

Implement the orchestrator in Python.

## Rationale

- The existing workflow contracts and validation code are already Python, so the orchestrator can reuse them directly without a new runtime boundary.
- Python keeps the service logic close to the schema-driven data model and the current test suite.
- For Day 2, a dependency-light Python implementation is faster to verify than introducing a second language, build toolchain, and service layout.
- Go would still be a reasonable choice for a production-only binary, but it would duplicate the current execution model and slow down the near-term integration work.

## Consequences

- The orchestrator can be composed from small repository, lease, and publisher interfaces without forcing a concrete driver today.
- The runtime stays aligned with the current validator and state machine.
- If the project later needs a standalone binary, the orchestration contract can be ported once the state transitions settle.

## Graph Validation Logic

The workflow validator already performs cycle detection before execution starts:

```python
cycle_path = _find_cycle(adjacency, entry_node_id)
if cycle_path:
    cycle_text = " -> ".join(cycle_path + [cycle_path[0]])
    raise WorkflowValidationError(f"workflow contains a cycle: {cycle_text}")
```

That check is paired with the unreachable-node scan so the orchestrator never starts a graph that is topologically unsafe.

## Orchestrator Execution Loop

The execution path acquires the Redis lease, transitions the execution to `RUNNING`, and publishes the first task assignment to NATS JetStream:

```python
workflow_definition = definition_repository.load(workflow_id)
validate_workflow_graph(workflow_definition.to_validation_payload())

execution = execution_repository.create(pending_execution)
lease = lease_manager.acquire(
    f"workflow-execution:{execution.execution_id}",
    orchestrator_agent_id,
    lease_ttl_seconds,
)
if lease is None:
    raise LeaseUnavailableError(...)

running_execution = execution_repository.transition_pending_to_running(...)
publisher.publish("nexus.task-assignment", message.to_dict())
```

The important ordering is: validate first, create the durable execution row, acquire the lease, transition `PENDING` to `RUNNING`, then publish the entry-node assignment.