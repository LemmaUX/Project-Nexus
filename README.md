# Nexus

Nexus is a Day 1 technical scaffold for a multi-agent orchestration platform with shared memory, durable execution state, and production-friendly messaging contracts.

The project is intentionally concrete and scoped to one useful workflow: an automated research pipeline where a planner, researcher, writer, and verifier collaborate over a shared execution state.

## What is included

- A JSON Schema contract for workflow graphs.
- A JSON Schema contract for workflow executions and recovery metadata.
- A JSON Schema contract for inter-agent messages with tracing and idempotency fields.
- A Python validator that rejects invalid graphs before runtime.
- A workflow execution state machine with valid transitions.
- An ADR that justifies NATS over Redpanda for this use case.
- Docker Compose infrastructure for PostgreSQL, Redis, and NATS.
- A Makefile with useful local commands.

## Local usage

1. Start the infrastructure with `make up`.
2. Run the validation suite with `make test`.
3. Inspect the contracts in `schemas/` and the Day 1 report in `docs/report-day-1.md`.

## Design principle

The platform is not a generic agent framework. It is designed around a single product-shaped workflow so the contracts stay simple, debuggable, and useful in production.
