"""
Prometheus metrics for Orchestrator service.
"""

from prometheus_client import Counter, Histogram, Gauge, Info

# Workflow metrics
WORKFLOW_EXECUTIONS_TOTAL = Counter(
    'nexus_orchestrator_workflow_executions_total',
    'Total workflow executions',
    ['workflow_id', 'status']
)

WORKFLOW_EXECUTION_DURATION = Histogram(
    'nexus_orchestrator_workflow_execution_duration_seconds',
    'Workflow execution duration',
    ['workflow_id'],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
)

WORKFLOW_STATE_TRANSITIONS = Counter(
    'nexus_orchestrator_workflow_state_transitions_total',
    'Workflow state transitions',
    ['from_state', 'to_state']
)

# Lease metrics
LEASE_ACQUISITIONS_TOTAL = Counter(
    'nexus_orchestrator_lease_acquisition_total',
    'Lease acquisition attempts',
    ['status']  # acquired, expired, failed
)

LEASE_DURATION = Histogram(
    'nexus_orchestrator_lease_duration_seconds',
    'Time lease was held',
    buckets=[1, 5, 10, 20, 30, 60, 120, 300]
)

# Performance metrics
MESSAGE_PUBLISH_LATENCY = Histogram(
    'nexus_orchestrator_message_publish_latency_seconds',
    'Time to publish message to NATS',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

ACTIVE_WORKFLOWS = Gauge(
    'nexus_orchestrator_active_workflows',
    'Number of currently active workflows',
    ['state']
)

# System info
SYSTEM_INFO = Info(
    'nexus_orchestrator',
    'Orchestrator system information'
)


def init_metrics(instance_id: str, version: str = "1.0.0"):
    """Initialize system info metrics."""
    SYSTEM_INFO.info({
        'instance_id': instance_id,
        'version': version,
    })
