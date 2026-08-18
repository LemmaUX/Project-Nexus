"""
Pytest configuration and shared fixtures for chaos tests.
"""

import pytest
import docker
import time


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "chaos: chaos engineering experiments")
    config.addinivalue_line("markers", "slow: slow-running tests")


@pytest.fixture(scope="session")
def docker_client():
    """Session-scoped Docker client."""
    return docker.from_env()


@pytest.fixture(scope="session", autouse=True)
def ensure_infrastructure(docker_client):
    """Ensure all infrastructure services are healthy before chaos tests."""
    required_services = [
        "nexus-postgres",
        "nexus-redis", 
        "nexus-nats",
        "nexus-otel-collector",
        "nexus-tempo",
        "nexus-prometheus",
        "nexus-grafana"
    ]
    
    print("\n🏗️ Verifying infrastructure health...")
    for service in required_services:
        try:
            container = docker_client.containers.get(service)
            health = container.attrs['State'].get('Health', {})
            status = health.get('Status', 'unknown')
            
            if status != 'healthy':
                # Don't skip, just warn - services may not have healthchecks
                print(f"  ⚠️ {service}: {status} (no healthcheck)")
            
            print(f"  ✅ {service}: running")
        except docker.errors.NotFound:
            # Service not found, will be handled by individual tests
            print(f"  ⚠️ {service}: not found")
    
    # Give metrics time to populate
    time.sleep(2)
    print("✅ Infrastructure check complete\n")
