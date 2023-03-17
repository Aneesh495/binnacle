"""
Binnacle Python Benchmarking Suite
Generates high-frequency MQTT load across multiple parallel client connections.
"""

import argparse
import time
import threading
from typing import List
from binnacle_client import BinnacleClient


def worker_task(
    worker_id: int,
    host: str,
    port: int,
    msg_count: int,
    payload_size: int,
    latencies: List[float],
) -> None:
    client_id = f"bench-worker-{worker_id}"
    client = BinnacleClient(client_id=client_id, host=host, port=port)
    try:
        client.connect()
    except Exception as e:
        print(f"Worker {worker_id} connection failed: {e}")
        return

    payload = b"X" * payload_size
    topic = f"bench/worker/{worker_id}"

    local_lats = []
    for _ in range(msg_count):
        t0 = time.perf_counter()
        client.publish(topic, payload, qos=0)
        t1 = time.perf_counter()
        local_lats.append((t1 - t0) * 1e6)  # microseconds

    client.disconnect()
    latencies.extend(local_lats)


def run_benchmark(host: str, port: int, clients: int, messages_per_client: int, payload_size: int) -> None:
    print(f"[Bench] Starting benchmark against {host}:{port}")
    print(f"  Clients: {clients}")
    print(f"  Messages/client: {messages_per_client}")
    print(f"  Total messages: {clients * messages_per_client}")
    print(f"  Payload size: {payload_size} bytes")

    all_latencies: List[float] = []
    threads: List[threading.Thread] = []

    t_start = time.perf_counter()
    for i in range(clients):
        t = threading.Thread(
            target=worker_task,
            args=(i, host, port, messages_per_client, payload_size, all_latencies),
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    t_end = time.perf_counter()

    total_msgs = len(all_latencies)
    elapsed = t_end - t_start
    throughput = total_msgs / max(0.0001, elapsed)

    print("\n[Bench Results]")
    print(f"  Total published: {total_msgs} messages")
    print(f"  Elapsed wall time: {elapsed:.3f} s")
    print(f"  Throughput: {throughput:.1f} msgs/sec")

    if all_latencies:
        all_latencies.sort()
        p50 = all_latencies[int(len(all_latencies) * 0.50)]
        p95 = all_latencies[int(len(all_latencies) * 0.95)]
        p99 = all_latencies[int(len(all_latencies) * 0.99)]
        print(f"  p50 latency: {p50:.1f} us")
        print(f"  p95 latency: {p95:.1f} us")
        print(f"  p99 latency: {p99:.1f} us")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Binnacle Python Benchmarking Harness")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host")
    parser.add_argument("--port", type=int, default=1883, help="Broker port")
    parser.add_argument("--clients", type=int, default=4, help="Client threads")
    parser.add_argument("--count", type=int, default=1000, help="Messages per client")
    parser.add_argument("--size", type=int, default=128, help="Payload bytes")
    args = parser.parse_args()

    run_benchmark(args.host, args.port, args.clients, args.count, args.size)
