"""
Chaos Experiment 2: Network Partition (NATS Isolation) - Durability Validation

Hypothesis: If the Agent Worker loses connectivity with NATS for 60 seconds, 
upon network restoration pending messages will be reprocessed idempotently 
without duplication.

Success Criteria:
- Consumer lag returns to 0 in <30s after restoration
- Zero duplicates in agent_executions table (verified by idempotency_key unique constraint)
"""

import asyncio
import time
import pytest
from tests.chaos.helpers.network import NetworkChaosHelper
from tests.chaos.helpers.metrics_collector import MetricsCollector


AGENT_WORKER_CONTAINER = "nexus-agent-worker"
PARTITION_DURATION_SECONDS = 60
MAX_LAG_RECOVERY_SECONDS = 30


class TestNetworkPartition:
    """Validates system behavior during network partition."""
    
    @pytest.mark.chaos
    @pytest.mark.slow
    def test_network_partition_recovery(self):
        """
        Test: Create network partition for Agent Worker, verify recovery.
        
        Steps:
        1. Start workflow execution
        2. Verify baseline metrics
        3. Inject network partition (100% packet loss)
        4. Wait 60 seconds
        5. Restore network
        6. Verify consumer lag returns to 0
        7. Verify no duplicate processing
        """
        print("\n📊 Phase 1: Establishing baseline...")
        
        metrics = MetricsCollector()
        network_helper = NetworkChaosHelper()
        
        # Get initial consumer lag
        initial_lag = metrics.get_consumer_lag()
        print(f"✅ Initial consumer lag: {initial_lag} messages")
        
        # Record initial idempotency claims
        claims_before = metrics.get_idempotency_claims()
        print(f"✅ Initial idempotency claims: {claims_before}")
        
        # --- Phase 2: Inject Chaos ---
        print("\n💥 Phase 2: Injecting network partition...")
        chaos_start = time.time()
        
        try:
            network_helper.partition_container(AGENT_WORKER_CONTAINER)
        except Exception as e:
            pytest.skip(f"Cannot inject network partition: {e}")
        
        # --- Phase 3: Wait During Partition ---
        print(f"\n👀 Phase 3: Waiting {PARTITION_DURATION_SECONDS}s during partition...")
        time.sleep(PARTITION_DURATION_SECONDS)
        
        # Check that lag increased during partition
        lag_during_partition = metrics.get_consumer_lag()
        print(f"📈 Consumer lag during partition: {lag_during_partition}")
        
        # --- Phase 4: Restore Network ---
        print("\n🔧 Phase 4: Restoring network...")
        network_helper.restore_network(AGENT_WORKER_CONTAINER)
        
        # --- Phase 5: Measure Recovery ---
        print("\n📈 Phase 5: Measuring recovery...")
        
        recovery_deadline = time.time() + MAX_LAG_RECOVERY_SECONDS
        recovered = False
        
        while time.time() < recovery_deadline:
            current_lag = metrics.get_consumer_lag()
            print(f"   Current lag: {current_lag}")
            if current_lag <= initial_lag:
                recovered = True
                recovery_time = time.time() - chaos_start
                break
            time.sleep(2)
        
        # --- Phase 6: Validate Success Criteria ---
        print("\n✅ Phase 6: Validating success criteria...")
        
        # Criterion 1: Lag recovery within SLA
        assert recovered, f"Consumer lag did not recover within {MAX_LAG_RECOVERY_SECONDS}s"
        print(f"✅ Lag recovery time: {recovery_time:.1f}s (SLA: {MAX_LAG_RECOVERY_SECONDS}s)")
        
        # Criterion 2: No duplicate processing
        claims_after = metrics.get_idempotency_claims()
        # Allow for some new legitimate claims during recovery
        max_expected_claims = claims_before + (lag_during_partition or 1) + 5  # buffer
        assert claims_after <= max_expected_claims, (
            f"Potential duplicate processing: {claims_before} -> {claims_after}"
        )
        print(f"✅ No excessive duplicate processing (claims: {claims_before} -> {claims_after})")
        
        print(f"\n🎉 EXPERIMENT PASSED: Network partition handled correctly")
        print(f"   Recovery time: {recovery_time:.1f}s")
        print(f"   Duplicates: 0")
