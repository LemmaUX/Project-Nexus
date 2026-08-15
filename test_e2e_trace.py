import asyncio
import sys
sys.path.insert(0, "src")

from nexus.orchestrator import create_orchestrator, WorkflowDefinition
from nexus.agent_worker import create_agent_worker, TaskAssignment
from datetime import datetime, timezone, timedelta

# Mock implementations
class MockDefinitionRepo:
    def load(self, workflow_id):
        return WorkflowDefinition(
            workflow_id="test-workflow",
            name="Test Research Pipeline",
            entry_node_id="node-1",
            nodes=[{"id": "node-1", "kind": "agent", "agent_role": "researcher", "output_schema_ref": "test"}],
            edges=[],
            terminal_node_ids=[],
        )

class MockExecutionRepo:
    def create(self, execution):
        return execution
    def transition_pending_to_running(self, execution_id, lease_owner, lease_expires_at, heartbeat_at, expected_version):
        from nexus.orchestrator import WorkflowExecutionRecord, WorkflowExecutionState
        return WorkflowExecutionRecord(
            execution_id=execution_id,
            workflow_id="test-workflow",
            state=WorkflowExecutionState.RUNNING,
            current_node_id="node-1",
            last_committed_node_id=None,
            version=1,
            lease_owner=lease_owner,
            lease_expires_at=lease_expires_at,
            heartbeat_at=heartbeat_at,
            resume_token=None,
            human_input_required=False,
            failure_reason=None,
            created_at=heartbeat_at,
            updated_at=heartbeat_at,
        )

class MockLeaseManager:
    def acquire(self, lease_key, owner_id, ttl_seconds):
        from nexus.orchestrator import LeaseToken
        return LeaseToken(
            lease_key=lease_key,
            owner_id=owner_id,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        )

class MockPublisher:
    def publish(self, subject, message, headers=None):
        print(f"📨 Published to {subject}: {message.get('message_type', 'unknown')}")

class MockIdempotencyRepo:
    def claim(self, idempotency_key, execution_id, message_id):
        return True

async def main():
    print("🚀 Iniciando test end-to-end de trazabilidad...\n")
    
    # 1. Iniciar Orquestador
    orchestrator = create_orchestrator(
        definition_repository=MockDefinitionRepo(),
        execution_repository=MockExecutionRepo(),
        lease_manager=MockLeaseManager(),
        publisher=MockPublisher(),
    )
    
    execution = orchestrator.start_workflow("test-workflow")
    print(f"✅ Workflow started: {execution.execution_id}\n")
    
    # 2. Simular Agent Worker procesando el mensaje
    worker = create_agent_worker(
        idempotency_repository=MockIdempotencyRepo(),
        publisher=MockPublisher(),
    )
    
    assignment = TaskAssignment(
        message_id="msg-001",
        correlation_id=execution.execution_id,
        parent_span_id=None,
        sender_agent_id="orchestrator",
        recipient_agent_id="researcher",
        payload_schema_ref="test",
        idempotency_key=f"{execution.execution_id}:node-1",
        timestamp=datetime.now(timezone.utc),
        message_type="TASK_ASSIGNMENT",
        payload={
            "execution_id": execution.execution_id,
            "workflow_id": "test-workflow",
            "workflow_name": "Test Research Pipeline",
            "node_id": "node-1",
            "agent_role": "researcher",
        }
    )
    
    result = worker.process_assignment(assignment)
    print(f"✅ Agent processed task: {result}\n")
    
    print("=" * 60)
    print("🔍 PRÓXIMOS PASOS:")
    print("=" * 60)
    print("\n1. Levanta el stack completo:")
    print("   $ docker compose down -v")
    print("   $ docker compose up -d")
    print("\n2. Espera 30 segundos a que todos los servicios estén healthy")
    print("\n3. Abre Grafana:")
    print("   URL: http://localhost:3000")
    print("   Usuario: admin")
    print("   Password: admin")
    print("\n4. Busca la traza:")
    print("   - Ve a Explore → Tempo")
    print(f"   - Filtra por correlation_id: {execution.execution_id}")
    print("   - Deberías ver spans de orchestrator y agent")
    print("\n5. Valida Redis memory config:")
    print("   $ docker exec nexus-redis redis-cli CONFIG GET maxmemory")
    print("   Salida esperada: 536870912 (512MB)")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())
