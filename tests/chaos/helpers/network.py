"""
Network chaos injection helpers using tc (traffic control).
"""

import subprocess
from typing import Optional


class NetworkChaosHelper:
    """Helper class for injecting network chaos using tc netem."""
    
    @staticmethod
    def get_container_pid(container_name: str) -> int:
        """Get the PID of a Docker container's main process."""
        import docker
        client = docker.from_env()
        container = client.containers.get(container_name)
        return container.attrs['State']['Pid']
    
    @staticmethod
    def add_network_loss(container_name: str, loss_percentage: int = 100, device: str = "eth0"):
        """
        Add network packet loss to a container.
        
        Args:
            container_name: Name of the Docker container
            loss_percentage: Percentage of packets to drop (0-100)
            device: Network interface device name
        """
        pid = NetworkChaosHelper.get_container_pid(container_name)
        
        # Add qdisc with netem loss
        cmd = f"nsenter -t {pid} -n tc qdisc add dev {device} root netem loss {loss_percentage}%"
        try:
            subprocess.run(cmd, shell=True, check=True, capture_output=True)
            print(f"🔴 Network loss injected: {loss_percentage}% on {container_name}")
        except subprocess.CalledProcessError as e:
            # qdisc might already exist, try to replace it
            NetworkChaosHelper.replace_network_loss(container_name, loss_percentage, device)
    
    @staticmethod
    def replace_network_loss(container_name: str, loss_percentage: int = 100, device: str = "eth0"):
        """Replace existing qdisc with new loss settings."""
        pid = NetworkChaosHelper.get_container_pid(container_name)
        
        # Delete existing qdisc first
        del_cmd = f"nsenter -t {pid} -n tc qdisc del dev {device} root 2>/dev/null || true"
        subprocess.run(del_cmd, shell=True, capture_output=True)
        
        # Add new qdisc
        cmd = f"nsenter -t {pid} -n tc qdisc add dev {device} root netem loss {loss_percentage}%"
        subprocess.run(cmd, shell=True, check=True, capture_output=True)
        print(f"🔴 Network loss replaced: {loss_percentage}% on {container_name}")
    
    @staticmethod
    def remove_network_chaos(container_name: str, device: str = "eth0"):
        """Remove all network chaos from a container."""
        pid = NetworkChaosHelper.get_container_pid(container_name)
        
        cmd = f"nsenter -t {pid} -n tc qdisc del dev {device} root"
        try:
            subprocess.run(cmd, shell=True, check=True, capture_output=True)
            print(f"🟢 Network chaos removed from {container_name}")
        except subprocess.CalledProcessError:
            print(f"⚠️ No qdisc to delete on {container_name}")
    
    @staticmethod
    def add_latency(container_name: str, latency_ms: int, device: str = "eth0"):
        """Add network latency to a container."""
        pid = NetworkChaosHelper.get_container_pid(container_name)
        
        cmd = f"nsenter -t {pid} -n tc qdisc add dev {device} root netem delay {latency_ms}ms"
        try:
            subprocess.run(cmd, shell=True, check=True, capture_output=True)
            print(f"⏱️ Latency added: {latency_ms}ms to {container_name}")
        except subprocess.CalledProcessError:
            # Replace existing
            del_cmd = f"nsenter -t {pid} -n tc qdisc del dev {device} root 2>/dev/null || true"
            subprocess.run(del_cmd, shell=True, capture_output=True)
            cmd = f"nsenter -t {pid} -n tc qdisc add dev {device} root netem delay {latency_ms}ms"
            subprocess.run(cmd, shell=True, check=True, capture_output=True)
            print(f"⏱️ Latency replaced: {latency_ms}ms to {container_name}")
    
    @staticmethod
    def partition_container(container_name: str):
        """Create a complete network partition (100% packet loss)."""
        NetworkChaosHelper.add_network_loss(container_name, 100)
    
    @staticmethod
    def restore_network(container_name: str):
        """Restore normal network conditions."""
        NetworkChaosHelper.remove_network_chaos(container_name)
