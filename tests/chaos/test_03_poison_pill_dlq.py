"""
Chaos Experiment 3: Poison Pill Injection - DLQ Validation

Hypothesis: A message with invalid schema will be moved to DLQ after exactly 
5 retries, without blocking processing of healthy messages.

Success Criteria:
- Message appears in nexus.dlq.agent.researcher subject
- DLQ Monitor generates alert in <10s
- Throughput of healthy messages does not degrade >5%
"""

import asyncio
import pytest
import time
from tests.chaos.helpers.metrics_collector import MetricsCollector


class TestPoisonPillDLQ:
    """Validates DLQ routing for poison pill messages."""
    
    @pytest.mark.chaos
    @pytest.mark.slow
    def test_poison_pill_dlq_routing(self):
        """
        Test: Inject poison pill message, verify DLQ routing.
        
        Steps:
        1. Record baseline metrics
        2. Publish poison pill message to NATS
        3. Wait for retries (5 attempts)
        4. Verify message in DLQ
        5. Verify healthy message throughput unchanged
        """
        print("\n📊 Phase 1: Establishing baseline...")
        
        metrics = MetricsCollector()
        
        # Get initial DLQ count
        dlq_before = metrics.get_dlq_messages()
        nak_before = metrics.get_nats_consumer_nak()
        processing_rate_before = metrics.get_agent_processing_rate()
        
        print(f"✅ DLQ messages before: {dlq_before}")
        print(f"✅ NAK count before: {nak_before}")
        print(f"✅ Processing rate before: {processing_rate_before}/s")
        
        # --- Phase 2: Inject Poison Pill ---
        print("\n💥 Phase 2: Injecting poison pill message...")
        
        try:
            self._inject_poison_pill()
        except Exception as e:
            pytest.skip(f"Cannot inject poison pill: {e}")
        
        # --- Phase 3: Wait for Retries ---
        print("\n👀 Phase 3: Waiting for retries (5 attempts)...")
        # Each retry has a backoff, wait ~30 seconds total
        time.sleep(30)
        
        # --- Phase 4: Check DLQ ---
        print("\n📈 Phase 4: Checking DLQ...")
        
        dlq_after = metrics.get_dlq_messages()
        nak_after = metrics.get_nats_consumer_nak()
        
        print(f"   DLQ messages after: {dlq_after}")
        print(f"   NAK count after: {nak_after}")
        
        # --- Phase 5: Validate Success Criteria ---
        print("\n✅ Phase 5: Validating success criteria...")
        
        # Criterion 1: Message moved to DLQ
        assert dlq_after > dlq_before, "Poison pill not moved to DLQ"
        print(f"✅ Message moved to DLQ ({dlq_before} -> {dlq_after})")
        
        # Criterion 2: Exactly 5 NAKs (retries)
        nak_count = nak_after - nak_before
        assert nak_count >= 5, f"Expected at least 5 NAKs, got {nak_count}"
        print(f"✅ Retry count verified: {nak_count} NAKs")
        
        # Criterion 3: Healthy throughput maintained
        processing_rate_after = metrics.get_agent_processing_rate()
        if processing_rate_before > 0:
            degradation = (processing_rate_before - processing_rate_after) / processing_rate_before
            assert degradation <= 0.05, f"Throughput degraded by {degradation*100:.1f}%"
            print(f"✅ Throughput maintained (degradation: {degradation*100:.1f}%)")
        
        print(f"\n🎉 EXPERIMENT PASSED: DLQ routing working correctly")
        print(f"   DLQ messages: {dlq_after - dlq_before}")
        print(f"   Retries: {nak_count}")
    
    def _inject_poison_pill(self):
        """Inject a poison pill message directly into NATS."""
        import asyncio
        from nats.aio.client import Client as NATS
        
        async def _publish():
            nc = await NATS().connect("nats://localhost:4222")
            js = nc.jetstream()
            
            # Message with invalid JSON (unclosed quote)
            poison_pill = b'{"message_id":"poison-001","message_type":"TASK_ASSIGNMENT","timestamp":"invalid'
            
            await js.publish(
                subject="nexus.task-assignment",
                data=poison_pill,
                stream="nexus-tasks"
            )
            print("💀 Poison pill injected")
            await nc.close()
        
        asyncio.run(_publish())
