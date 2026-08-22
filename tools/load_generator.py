#!/usr/bin/env python3
"""
High-Performance Load Generator for Project Nexus
Generates sustained load against NATS JetStream with real-time metrics.

Usage:
    python tools/load_generator.py --tps 10000 --duration 600 --ramp-up 60
"""

from collections import deque
import argparse
import asyncio
import json
import time
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional
import statistics

from nats.aio.client import Client as NATS
from prometheus_client import Counter, Histogram, Gauge, start_http_server


# ============================================================================
# METRICS (Prometheus)
# ============================================================================

MESSAGES_PUBLISHED = Counter(
    'load_generator_messages_published_total',
    'Total messages published',
    ['status']
)

PUBLISH_LATENCY = Histogram(
    'load_generator_publish_latency_seconds',
    'Time to publish message to NATS',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

ACTIVE_PUBLISHERS = Gauge(
    'load_generator_active_publishers',
    'Number of active publisher tasks'
)

CURRENT_TPS = Gauge(
    'load_generator_current_tps',
    'Current transactions per second'
)


# ============================================================================
# MESSAGE GENERATION
# ============================================================================

@dataclass
class LoadGeneratorConfig:
    nats_url: str = "nats://localhost:4222"
    subject: str = "nexus.task-assignment"
    stream: str = "nexus-tasks"
    target_tps: int = 10000
    duration_seconds: int = 600
    ramp_up_seconds: int = 60
    metrics_port: int = 9100


class MessageGenerator:
    """Generates realistic message payloads with distribution."""
    
    WORKFLOW_TYPES = [
        ("research-pipeline", 0.6),
        ("analysis-pipeline", 0.3),
        ("validation-pipeline", 0.1),
    ]
    
    AGENT_ROLES = [
        ("researcher", 0.4),
        ("analyst", 0.3),
        ("verifier", 0.2),
        ("writer", 0.1),
    ]
    
    def __init__(self):
        self.message_count = 0
    
    def generate_message(self) -> dict:
        """Generate a realistic task assignment message."""
        self.message_count += 1
        
        # Select workflow type based on distribution
        workflow_type = self._weighted_choice(self.WORKFLOW_TYPES)
        agent_role = self._weighted_choice(self.AGENT_ROLES)
        
        # Generate unique identifiers
        execution_id = f"exec-{uuid.uuid4().hex[:12]}"
        message_id = f"msg-{uuid.uuid4().hex[:16]}"
        idempotency_key = f"{execution_id}:node-{self.message_count}"
        
        # Realistic payload sizes (80% small, 15% medium, 5% large)
        payload_size = self._generate_payload_size()
        
        return {
            "message_id": message_id,
            "correlation_id": execution_id,
            "parent_span_id": None,
            "sender_agent_id": "load-generator",
            "recipient_agent_id": agent_role,
            "payload_schema_ref": "schemas/task-assignment.schema.json",
            "idempotency_key": idempotency_key,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message_type": "TASK_ASSIGNMENT",
            "payload": {
                "execution_id": execution_id,
                "workflow_id": workflow_type,
                "workflow_name": f"{workflow_type}-v1",
                "node_id": f"node-{self.message_count % 100}",
                "agent_role": agent_role,
                "context": self._generate_context(payload_size),
            }
        }
    
    def _weighted_choice(self, options: list[tuple[str, float]]) -> str:
        """Select option based on weighted probability."""
        import random
        r = random.random()
        cumulative = 0.0
        for option, weight in options:
            cumulative += weight
            if r <= cumulative:
                return option
        return options[-1][0]
    
    def _generate_payload_size(self) -> str:
        """Generate payload size distribution."""
        import random
        r = random.random()
        if r < 0.8:
            return "small"
        elif r < 0.95:
            return "medium"
        else:
            return "large"
    
    def _generate_context(self, size: str) -> dict:
        """Generate context of specified size."""
        base_context = {
            "user_id": f"user-{uuid.uuid4().hex[:8]}",
            "session_id": f"session-{uuid.uuid4().hex[:12]}",
            "priority": "normal",
        }
        
        if size == "medium":
            base_context["metadata"] = {"key" + str(i): "value" * 10 for i in range(10)}
        elif size == "large":
            base_context["metadata"] = {"key" + str(i): "value" * 50 for i in range(50)}
            base_context["history"] = [{"event": f"event-{i}"} for i in range(20)]
        
        return base_context


# ============================================================================
# LOAD GENERATOR
# ============================================================================

class LoadGenerator:
    """High-performance load generator with ramp-up and metrics."""
    
    def __init__(self, config: LoadGeneratorConfig):
        self.config = config
        self.message_gen = MessageGenerator()
        self.nc: Optional[NATS] = None
        self.js = None
        self.running = False
        self.start_time: Optional[float] = None
        
        # Performance tracking - use deque with maxlen to prevent memory leaks
        self.latencies: deque[float] = deque(maxlen=10000)  # Keep last 10k samples
        self.published_count = 0
        self.failed_count = 0
    
    async def start(self):
        """Start the load generator."""
        # Start Prometheus metrics server
        start_http_server(self.config.metrics_port)
        print(f"📊 Metrics server started on port {self.config.metrics_port}")
        
        # Connect to NATS with proper error handling
        try:
            self.nc = NATS()
            await self.nc.connect(self.config.nats_url, max_reconnect_attempts=5)
            self.js = self.nc.jetstream()
            print(f"✅ Connected to NATS at {self.config.nats_url}")
        except Exception as e:
            print(f"❌ Failed to connect to NATS: {e}")
            return
        
        self.running = True
        self.start_time = time.time()
        
        # Start publishing tasks
        await self._run_load_test()
        
        # Cleanup
        await self.nc.close()
        self.running = False
    
    async def _run_load_test(self):
        """Execute the load test with ramp-up."""
        print(f"\n🚀 Starting load test:")
        print(f"   Target TPS: {self.config.target_tps}")
        print(f"   Duration: {self.config.duration_seconds}s")
        print(f"   Ramp-up: {self.config.ramp_up_seconds}s\n")
        
        end_time = self.start_time + self.config.duration_seconds
        
        # Create publisher tasks
        num_publishers = min(100, self.config.target_tps // 100)
        ACTIVE_PUBLISHERS.set(num_publishers)
        
        tasks = [
            asyncio.create_task(self._publisher_worker(i))
            for i in range(num_publishers)
        ]
        
        # Monitor progress
        monitor_task = asyncio.create_task(self._monitor_progress(end_time))
        
        # Wait for completion
        await asyncio.gather(*tasks, monitor_task)
        
        # Print final report
        self._print_final_report()
    
    async def _publisher_worker(self, worker_id: int):
        """Worker that publishes messages at target rate."""
        messages_per_second = self.config.target_tps / 100  # 100 workers
        interval = 1.0 / messages_per_second if messages_per_second > 0 else 1.0
        
        while self.running:
            # Calculate current TPS based on ramp-up
            elapsed = time.time() - self.start_time
            if elapsed < self.config.ramp_up_seconds:
                # Linear ramp-up
                ramp_factor = elapsed / self.config.ramp_up_seconds
                current_interval = interval / ramp_factor if ramp_factor > 0 else interval * 10
            else:
                current_interval = interval
            
            start = time.time()
            
            try:
                await self._publish_message()
                self.published_count += 1
                MESSAGES_PUBLISHED.labels(status='success').inc()
            except Exception as e:
                self.failed_count += 1
                MESSAGES_PUBLISHED.labels(status='failed').inc()
                print(f"⚠️ Worker {worker_id} publish failed: {e}")
            
            # Sleep to maintain target rate
            elapsed = time.time() - start
            sleep_time = max(0, current_interval - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
    
    async def _publish_message(self):
        """Publish a single message with latency tracking."""
        message = self.message_gen.generate_message()
        payload = json.dumps(message).encode('utf-8')
        
        start = time.time()
        
        # Publish to NATS JetStream
        await self.js.publish(
            subject=self.config.subject,
            data=payload,
            stream=self.config.stream
        )
        
        latency = time.time() - start
        self.latencies.append(latency)
        PUBLISH_LATENCY.observe(latency)
    
    async def _monitor_progress(self, end_time: float):
        """Monitor and report progress periodically."""
        last_count = 0
        last_time = time.time()
        
        while time.time() < end_time and self.running:
            await asyncio.sleep(5)
            
            current_time = time.time()
            elapsed = current_time - self.start_time
            duration = current_time - last_time
            
            # Calculate current TPS
            messages_in_period = self.published_count - last_count
            current_tps = messages_in_period / duration if duration > 0 else 0
            CURRENT_TPS.set(current_tps)
            
            # Calculate latency percentiles
            if self.latencies:
                recent_latencies = self.latencies[-1000:]  # Last 1000
                p50 = statistics.median(recent_latencies)
                p95 = statistics.quantiles(recent_latencies, n=20)[18]
                p99 = statistics.quantiles(recent_latencies, n=100)[98]
            else:
                p50 = p95 = p99 = 0
            
            # Progress report
            progress = (elapsed / self.config.duration_seconds) * 100
            print(f"[{progress:5.1f}%] TPS: {current_tps:6.0f} | "
                  f"Published: {self.published_count:8d} | "
                  f"Failed: {self.failed_count:5d} | "
                  f"Latency p50: {p50*1000:5.1f}ms p95: {p95*1000:5.1f}ms p99: {p99*1000:5.1f}ms")
            
            last_count = self.published_count
            last_time = current_time
    
    def _print_final_report(self):
        """Print final performance report."""
        duration = time.time() - self.start_time
        
        print("\n" + "="*80)
        print("📊 LOAD TEST FINAL REPORT")
        print("="*80)
        print(f"Duration:              {duration:.1f}s")
        print(f"Total Published:       {self.published_count:,}")
        print(f"Total Failed:          {self.failed_count:,}")
        print(f"Success Rate:          {(self.published_count / (self.published_count + self.failed_count) * 100):.2f}%")
        print(f"Average TPS:           {self.published_count / duration:.0f}")
        
        if self.latencies:
            print(f"\nLatency Distribution:")
            print(f"  Mean:                {statistics.mean(self.latencies)*1000:.2f}ms")
            print(f"  Median (p50):        {statistics.median(self.latencies)*1000:.2f}ms")
            print(f"  p95:                 {statistics.quantiles(self.latencies, n=20)[18]*1000:.2f}ms")
            print(f"  p99:                 {statistics.quantiles(self.latencies, n=100)[98]*1000:.2f}ms")
            print(f"  Max:                 {max(self.latencies)*1000:.2f}ms")
        
        print("="*80)


# ============================================================================
# MAIN
# ============================================================================

def parse_args():
    parser = argparse.ArgumentParser(description='Nexus Load Generator')
    parser.add_argument('--nats-url', default='nats://localhost:4222',
                       help='NATS server URL')
    parser.add_argument('--subject', default='nexus.task-assignment',
                       help='NATS subject to publish to')
    parser.add_argument('--stream', default='nexus-tasks',
                       help='NATS JetStream stream name')
    parser.add_argument('--tps', type=int, default=10000,
                       help='Target transactions per second')
    parser.add_argument('--duration', type=int, default=600,
                       help='Test duration in seconds')
    parser.add_argument('--ramp-up', type=int, default=60,
                       help='Ramp-up period in seconds')
    parser.add_argument('--metrics-port', type=int, default=9100,
                       help='Prometheus metrics port')
    return parser.parse_args()


async def main():
    args = parse_args()
    
    config = LoadGeneratorConfig(
        nats_url=args.nats_url,
        subject=args.subject,
        stream=args.stream,
        target_tps=args.tps,
        duration_seconds=args.duration,
        ramp_up_seconds=args.ramp_up,
        metrics_port=args.metrics_port,
    )
    
    generator = LoadGenerator(config)
    await generator.start()


if __name__ == "__main__":
    asyncio.run(main())
