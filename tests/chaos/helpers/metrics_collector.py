"""
Metrics collector for Prometheus-based observability during chaos tests.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta


class MetricsCollector:
    """Helper class for querying Prometheus metrics during chaos experiments."""
    
    def __init__(self, prometheus_url: str = "http://localhost:9090"):
        from prometheus_api_client import PrometheusConnect
        self.prometheus = PrometheusConnect(url=prometheus_url, disable_ssl=True)
    
    def query_metric(self, metric_name: str) -> float:
        """Query current value of a Prometheus metric."""
        try:
            result = self.prometheus.custom_query(query=metric_name)
            if not result:
                return 0.0
            return float(result[0]['value'][1])
        except Exception as e:
            print(f"⚠️ Error querying metric {metric_name}: {e}")
            return 0.0
    
    def query_metric_range(
        self, 
        metric_name: str, 
        start_time: datetime, 
        end_time: datetime,
        step: str = "1s"
    ) -> List[Dict[str, Any]]:
        """Query metric values over a time range."""
        try:
            result = self.prometheus.custom_query_range(
                query=metric_name,
                start_time=start_time,
                end_time=end_time,
                step=step
            )
            return result if result else []
        except Exception as e:
            print(f"⚠️ Error querying range metric {metric_name}: {e}")
            return []
    
    def get_workflow_state(self, state: str = "RUNNING") -> float:
        """Get count of workflows in a specific state."""
        return self.query_metric(f'workflow_execution_state{{state="{state}"}}')
    
    def get_lease_acquisitions(self, status: str = "acquired") -> float:
        """Get count of lease acquisitions by status."""
        return self.query_metric(f'nexus_orchestrator_lease_acquisition_total{{status="{status}"}}')
    
    def get_recovery_attempts(self) -> float:
        """Get count of recovery manager attempts."""
        return self.query_metric('nexus_recovery_manager_recovery_attempts_total')
    
    def get_idempotency_claims(self) -> float:
        """Get count of idempotency claims."""
        return self.query_metric('idempotency_claims_total')
    
    def get_dlq_messages(self, agent: str = "researcher") -> float:
        """Get count of messages in DLQ for an agent."""
        return self.query_metric(f'nexus_dlq_messages_total{{agent="{agent}"}}')
    
    def get_consumer_lag(self) -> float:
        """Get NATS consumer lag in messages."""
        return self.query_metric('nats_consumer_lag_messages')
    
    def get_otel_queue_size(self) -> float:
        """Get OTel exporter queue size."""
        return self.query_metric('otel_exporter_queue_size')
    
    def get_otel_failed_spans(self) -> float:
        """Get count of failed OTel span exports."""
        return self.query_metric('otel_exporter_failed_spans_total')
    
    def get_otel_sent_spans(self) -> float:
        """Get count of successfully sent OTel spans."""
        return self.query_metric('otel_exporter_sent_spans_total')
    
    def get_nats_consumer_nak(self) -> float:
        """Get count of NAK responses from NATS consumer."""
        return self.query_metric('nats_consumer_nak_total')
    
    def get_agent_processing_rate(self) -> float:
        """Get agent message processing rate."""
        return self.query_metric('agent_message_processing_rate')
    
    def get_p99_latency(self) -> float:
        """Get p99 latency in milliseconds."""
        return self.query_metric('workflow_p99_latency_ms')
    
    def capture_snapshot(self, timestamp: Optional[datetime] = None) -> Dict[str, float]:
        """Capture a snapshot of key metrics at a point in time."""
        if timestamp is None:
            timestamp = datetime.now()
        
        return {
            'timestamp': timestamp.isoformat(),
            'workflows_running': self.get_workflow_state('RUNNING'),
            'workflows_completed': self.get_workflow_state('COMPLETED'),
            'workflows_crashed': self.get_workflow_state('CRASHED'),
            'lease_acquisitions': self.get_lease_acquisitions('acquired'),
            'lease_expired': self.get_lease_acquisitions('expired'),
            'recovery_attempts': self.get_recovery_attempts(),
            'idempotency_claims': self.get_idempotency_claims(),
            'dlq_messages': self.get_dlq_messages(),
            'consumer_lag': self.get_consumer_lag(),
            'otel_queue_size': self.get_otel_queue_size(),
            'otel_failed_spans': self.get_otel_failed_spans(),
        }
    
    def wait_for_metric_condition(
        self, 
        metric_name: str, 
        condition: callable,
        timeout: int = 30,
        poll_interval: float = 1.0
    ) -> bool:
        """Wait for a metric to satisfy a condition."""
        start_time = datetime.now()
        
        while (datetime.now() - start_time).total_seconds() < timeout:
            value = self.query_metric(metric_name)
            if condition(value):
                return True
            time.sleep(poll_interval)
        
        return False
