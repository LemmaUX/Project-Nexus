# ADR 001: Message Broker Selection

## Status

Accepted.

## Context

Nexus coordinates multiple specialized agents that need two different communication patterns:

- Synchronous request-reply for coordination steps such as `planner -> researcher` handoffs and human approval gates.
- Durable asynchronous delivery for retries, fan-out, and recovery after partial failures.

The broker must support low-friction local development, simple mental models, and production-friendly persistence.

## Decision

Use NATS with JetStream as the message broker for Nexus.

## Rationale

NATS is a better fit than Redpanda for this project because the dominant interaction style is agent-to-agent coordination, not event warehousing.

- Request-reply is a first-class pattern in NATS, which matches agent delegation and acknowledgement flows.
- JetStream provides durability, replay, and at-least-once delivery without forcing Kafka-style topic design.
- The broker is lightweight enough for local development and demo environments.
- The operational model stays simple while still allowing backpressure, consumer retries, and dead-letter handling.

Redpanda would be the stronger choice if Nexus were primarily a high-throughput event pipeline with long retention and broad streaming analytics. That is not the core problem here.

## Consequences

- Inter-agent messages are modeled as typed envelopes with idempotency keys and correlation metadata.
- Durable state still lives in PostgreSQL, while Redis is reserved for leases, locks, and ephemeral execution cache.
- The architecture stays focused on one useful workflow instead of becoming a general-purpose streaming platform.
