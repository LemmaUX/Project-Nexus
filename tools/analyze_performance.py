#!/usr/bin/env python3
"""
Analyze performance metrics and identify bottlenecks.
"""

import requests
import json
from datetime import datetime, timedelta

try:
    from prometheus_api_client import PrometheusConnect
except ImportError:
    print("Warning: prometheus-api-client not installed. Using direct API calls.")
    PrometheusConnect = None


def query_prometheus(prom_url: str, query: str) -> float:
    """Query Prometheus and return scalar result."""
    if PrometheusConnect:
        try:
            prom = PrometheusConnect(url=prom_url, disable_ssl=True)
            result = prom.custom_query(query=query)
            if result:
                return float(result[0]['value'][1])
        except Exception as e:
            print(f"  Warning: Could not query Prometheus: {e}")
            return 0.0
    
    # Fallback to direct API call
    try:
        response = requests.get(
            f"{prom_url}/api/v1/query",
            params={"query": query},
            timeout=5
        )
        if response.status_code == 200:
            data = response.json()
            if data.get('data') and data['data'].get('result'):
                return float(data['data']['result'][0]['value'][1])
    except Exception as e:
        print(f"  Warning: Direct API query failed: {e}")
    
    return 0.0


def analyze_bottlenecks(prom_url: str = "http://localhost:9090"):
    """Analyze system performance and identify bottlenecks."""
    
    print("="*80)
    print("🔍 PERFORMANCE BOTTLENECK ANALYSIS")
    print("="*80)
    
    # 1. Check overall throughput
    avg_tps = query_prometheus(prom_url, "avg(load_generator_current_tps)")
    print(f"\n📊 Overall Performance:")
    print(f"   Average TPS: {avg_tps:.0f}")
    
    # 2. Check latencies
    p50 = query_prometheus(prom_url, 
        "histogram_quantile(0.50, rate(load_generator_publish_latency_seconds_bucket[5m]))")
    p95 = query_prometheus(prom_url,
        "histogram_quantile(0.95, rate(load_generator_publish_latency_seconds_bucket[5m]))")
    p99 = query_prometheus(prom_url,
        "histogram_quantile(0.99, rate(load_generator_publish_latency_seconds_bucket[5m]))")
    
    print(f"\n⏱️  Latency Distribution:")
    print(f"   p50: {p50*1000:.2f}ms")
    print(f"   p95: {p95*1000:.2f}ms")
    print(f"   p99: {p99*1000:.2f}ms")
    
    # 3. Check error rate
    error_rate = query_prometheus(prom_url,
        "rate(load_generator_messages_published_total{status='failed'}[5m]) / "
        "rate(load_generator_messages_published_total[5m]) * 100")
    
    print(f"\n❌ Error Rate: {error_rate:.2f}%")
    
    # 4. Check consumer lag
    consumer_lag = query_prometheus(prom_url, "nexus_agent_consumer_lag_messages")
    print(f"\n📥 Consumer Lag: {consumer_lag:.0f} messages")
    
    # 5. Identify bottlenecks
    print(f"\n🎯 BOTTLENECK IDENTIFICATION:")
    
    bottlenecks = []
    
    if p99 > 0.1:  # 100ms
        bottlenecks.append({
            'component': 'NATS Publish',
            'issue': f'High p99 latency ({p99*1000:.0f}ms > 100ms)',
            'recommendation': 'Increase NATS batch size or add more publisher workers'
        })
    
    if consumer_lag > 1000:
        bottlenecks.append({
            'component': 'Agent Worker',
            'issue': f'High consumer lag ({consumer_lag:.0f} messages)',
            'recommendation': 'Scale Agent Worker horizontally or optimize processing'
        })
    
    if error_rate > 1.0:
        bottlenecks.append({
            'component': 'System',
            'issue': f'High error rate ({error_rate:.2f}%)',
            'recommendation': 'Check logs for root cause, may be resource exhaustion'
        })
    
    if avg_tps < 9000:
        bottlenecks.append({
            'component': 'System',
            'issue': f'Below target TPS ({avg_tps:.0f} < 10000)',
            'recommendation': 'Profile CPU usage, check for locks or contention'
        })
    
    if bottlenecks:
        for i, bn in enumerate(bottlenecks, 1):
            print(f"\n   {i}. {bn['component']}:")
            print(f"      Issue: {bn['issue']}")
            print(f"      Fix: {bn['recommendation']}")
    else:
        print("\n   ✅ No critical bottlenecks detected!")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    analyze_bottlenecks()
