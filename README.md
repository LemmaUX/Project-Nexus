# Nexus

Nexus is a workflow orchestration platform for multi-agent systems with shared memory, durable execution state, and production-friendly messaging contracts.

The project is intentionally scoped to one concrete workflow: an automated research pipeline where a planner, researcher, writer, and verifier collaborate over shared execution state.

## What is included

- JSON Schema contracts for workflow graphs, workflow executions, and inter-agent messages.
- A Python validator that rejects invalid graphs before runtime.
- A workflow execution state machine with valid transitions.
- A Python workflow orchestrator that validates topology, creates executions, acquires Redis leases, and publishes task assignments to NATS JetStream.
- ADRs covering the message broker choice and the orchestrator language selection.
- Docker Compose infrastructure for PostgreSQL, Redis, and NATS.
- A Makefile with useful local commands.

## Local usage

1. Start the infrastructure with `make up`.
2. Run the validation suite with `make test`.
3. Inspect the contracts in `schemas/` and the design notes in `docs/`.

## Design principle

The platform is not a generic agent framework. It is designed around a single product-shaped workflow so the contracts stay simple, debuggable, and useful in production.