"""
Docker utility helpers for chaos testing.
"""

import time
from typing import Optional


class DockerUtils:
    """Utility class for Docker container operations."""
    
    def __init__(self):
        import docker
        self.client = docker.from_env()
    
    def get_container(self, container_name: str):
        """Get a Docker container by name."""
        return self.client.containers.get(container_name)
    
    def kill_container(self, container_name: str):
        """Force kill a container."""
        container = self.get_container(container_name)
        container.kill()
        print(f"💀 Killed container: {container_name}")
    
    def restart_container(self, container_name: str):
        """Restart a container."""
        container = self.get_container(container_name)
        container.restart()
        print(f"🔄 Restarted container: {container_name}")
    
    def start_container(self, container_name: str):
        """Start a stopped container."""
        container = self.get_container(container_name)
        container.start()
        print(f"▶️ Started container: {container_name}")
    
    def stop_container(self, container_name: str, timeout: int = 10):
        """Stop a container gracefully."""
        container = self.get_container(container_name)
        container.stop(timeout=timeout)
        print(f"⏹️ Stopped container: {container_name}")
    
    def is_container_healthy(self, container_name: str) -> bool:
        """Check if a container is healthy."""
        try:
            container = self.get_container(container_name)
            health = container.attrs['State'].get('Health', {})
            status = health.get('Status', 'unknown')
            return status == 'healthy'
        except Exception:
            # Container might not have healthcheck
            container = self.get_container(container_name)
            return container.attrs['State']['Running']
    
    def wait_for_healthy(self, container_name: str, timeout: int = 30, poll_interval: float = 1.0) -> bool:
        """Wait for a container to become healthy."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if self.is_container_healthy(container_name):
                print(f"✅ Container {container_name} is healthy")
                return True
            time.sleep(poll_interval)
        
        raise TimeoutError(f"Container {container_name} did not become healthy within {timeout}s")
    
    def wait_for_running(self, container_name: str, timeout: int = 30, poll_interval: float = 1.0) -> bool:
        """Wait for a container to be in running state."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                container = self.get_container(container_name)
                if container.attrs['State']['Running']:
                    print(f"✅ Container {container_name} is running")
                    return True
            except Exception:
                pass
            time.sleep(poll_interval)
        
        raise TimeoutError(f"Container {container_name} did not start within {timeout}s")
    
    def get_container_logs(self, container_name: str, tail: int = 100) -> str:
        """Get recent logs from a container."""
        container = self.get_container(container_name)
        logs = container.logs(tail=tail).decode('utf-8')
        return logs
    
    def list_containers(self, prefix: Optional[str] = None) -> list:
        """List all containers, optionally filtered by prefix."""
        containers = self.client.containers.list(all=True)
        if prefix:
            containers = [c for c in containers if c.name.startswith(prefix)]
        return containers
    
    def get_container_stats(self, container_name: str) -> dict:
        """Get resource usage stats for a container."""
        container = self.get_container(container_name)
        stats = container.stats(stream=False)
        return stats
