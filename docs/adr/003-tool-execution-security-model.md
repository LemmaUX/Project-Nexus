# ADR 003: Tool Execution Security Model

## Status

Proposed.

## Context

Day 3 adds the Agent Worker. It receives a `TASK_ASSIGNMENT` from NATS JetStream, decides whether the task has already been processed, runs the model, executes tools, and publishes a `TASK_RESULT`.

That boundary is dangerous if it treats model output as executable code. The worker must never allow an LLM to issue raw shell commands, read arbitrary files, or call unauthorized network endpoints.

## Decision

Use a closed-world tool registry with deny-by-default validation, and keep the LLM behind a structured tool-invocation interface.

The worker may request a tool only by name and JSON-like arguments. The sandbox validates the request before any execution happens. If a tool is not explicitly registered, it is rejected. Shell execution is not registered at all.

## Security Controls

- Only approved tool names are exposed to the worker.
- The sandbox rejects command-shaped arguments such as `command`, `argv`, `shell`, `executable`, and `script`.
- Network access is limited to an allowlist of hostnames and only `http` or `https` URLs.
- Idempotency is enforced in PostgreSQL before any model or tool call so redeliveries do not trigger duplicate LLM usage.
- The worker only publishes structured `TASK_RESULT` envelopes back to NATS; it does not emit arbitrary process output.

## PostgreSQL Idempotency Pattern

The claim should be atomic so one redelivery cannot pass two workers through the model at once:

```sql
INSERT INTO agent_task_idempotency (idempotency_key, execution_id, message_id, created_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (idempotency_key) DO NOTHING;
```

If the insert affects zero rows, the worker treats the message as already processed and acknowledges it without re-running the model or the tool.

## JetStream Worker Loop

The worker should use a pull consumer so backpressure comes from the fetch call instead of an unbounded push stream:

```python
consumer = await js.pull_subscribe(
    "nexus.task-assignment",
    durable="research-agent-worker",
    stream="nexus-tasks",
)

while True:
    batch = await consumer.fetch(batch=10, timeout=1.0)
    for message in batch:
        await handle_assignment(message)
```

That layout keeps the consumer bounded, lets the worker ack only after the result is published, and makes duplicate delivery a data problem instead of a compute problem.

## Consequences

- The worker can safely support additional tools later by registering them explicitly.
- Model output remains structured and auditable.
- A command-injection prompt like `rm -rf /` has no execution path because no shell tool exists in the registry.
- Unauthorized network access is rejected before any outbound request is made.