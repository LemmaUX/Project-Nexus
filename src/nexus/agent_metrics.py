"""
Prometheus metrics for Agent Worker service.
"""

from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Message processing
MESSAGES_PROCESSED_TOTAL = Counter(
    'nexus_agent_messages_processed_total',
    'Total messages processed',
    ['agent_role', 'status']  # success, failed, poison_pill
)

MESSAGE_PROCESSING_DURATION = Histogram(
    'nexus_agent_message_processing_duration_seconds',
    'Message processing duration',
    ['agent_role', 'stage'],  # total, idempotency_check, tool_execution, compose_result
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]
)

# Tool execution
TOOL_EXECUTIONS_TOTAL = Counter(
    'nexus_agent_tool_executions_total',
    'Tool executions',
    ['tool_name', 'status']
)

TOOL_EXECUTION_DURATION = Histogram(
    'nexus_agent_tool_execution_duration_seconds',
    'Tool execution duration',
    ['tool_name'],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

# Idempotency
IDEMPOTENCY_CHECKS_TOTAL = Counter(
    'nexus_agent_idempotency_checks_total',
    'Idempotency checks',
    ['result']  # new, duplicate
)

# NATS consumer
CONSUMER_LAG = Gauge(
    'nexus_agent_consumer_lag_messages',
    'NATS consumer lag in messages'
)

CONSUMER_FETCH_DURATION = Histogram(
    'nexus_agent_consumer_fetch_duration_seconds',
    'Time to fetch messages from NATS',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

# Error tracking
ERRORS_TOTAL = Counter(
    'nexus_agent_errors_total',
    'Errors encountered',
    ['error_type']
)


def init_metrics(agent_role: str = "researcher", port: int = 9092):
    """Start Prometheus metrics HTTP server."""
    start_http_server(port)
