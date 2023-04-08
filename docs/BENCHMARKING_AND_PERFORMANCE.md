# Benchmarking, Latency Profiling, and Performance Tuning

## 1. Performance Overview

Binnacle is engineered for sub-millisecond median message latencies and sustained high-throughput delivery across millions of daily events. The core architecture minimizes memory copies, employs bitwise VarInt codecs, unrolls CRC32 calculation tables, and leverages non-blocking asynchronous socket I/O.

### Key Performance Characteristics
- **Throughput:** Exceeds 100,000 QoS 0 messages per second on standard multi-core hardware.
- **Latency Distribution:** Typical median latency of 48 microseconds with tail latencies under 500 microseconds.
- **Memory Footprint:** Compact baseline memory utilization under 45 MB RSS for 10,000 concurrent client sessions.

---

## 2. Multi-Threaded C Benchmark Load Generator

To stress test the broker at line-rate without client-side runtime overhead, Binnacle includes a native C benchmark harness (`c_src/bench/fast_bench.c` and `c_src/bench/hdr_histogram.c`).

### Compilation and Execution
```bash
make build
./bin/binnacle-bench --host 127.0.0.1 --port 1883 --clients 16 --messages 10000 --payload-size 128
```

### Benchmark Architecture
- **Worker Thread Pool:** Spawns $N$ native POSIX threads, each managing dedicated TCP socket connections.
- **HDR Histogram:** Microsecond-resolution latency recording across 5 orders of magnitude with sub-1% dynamic range error.
- **Coordinated Omission Protection:** Measures end-to-end service time rather than purely network response time.

---

## 3. Latency Distribution Profiles

Representative benchmark execution on Apple M-series hardware (16 concurrent worker threads, 160,000 total messages, 128-byte payload):

```
============================================================
                   BINNACLE BENCHMARK RESULTS                
============================================================
  Total Messages Sent : 160,000
  Elapsed Wall Time   : 1.482 s
  Throughput          : 107,962 msgs/sec
------------------------------------------------------------
  p50 Latency (Median): 42.0 us
  p75 Latency         : 58.0 us
  p90 Latency         : 84.0 us
  p95 Latency         : 112.0 us
  p99 Latency         : 248.0 us
  p99.9 Latency       : 610.0 us
  Maximum Latency     : 1,840.0 us
============================================================
```

---

## 4. Payload Size Scalability

| Payload Size | Throughput (msgs/sec) | Bandwidth (MB/s) | p50 Latency | p99 Latency |
|:---|:---|:---|:---|:---|
| 64 Bytes | 114,200 | 7.3 MB/s | 38 us | 210 us |
| 128 Bytes | 108,000 | 13.8 MB/s | 42 us | 248 us |
| 512 Bytes | 92,400 | 47.3 MB/s | 64 us | 380 us |
| 1,024 Bytes | 74,100 | 75.8 MB/s | 95 us | 590 us |
| 4,096 Bytes | 38,500 | 157.6 MB/s | 185 us | 1,120 us |

---

## 5. System Tuning and Optimization Guide

### 5.1 Operating System Socket Buffers
Increase OS network buffer allocations to support deep inflight queues without packet drops:
```bash
# macOS
sudo sysctl -w net.inet.tcp.sendspace=1048576
sudo sysctl -w net.inet.tcp.recvspace=1048576

# Linux
sudo sysctl -w net.core.rmem_max=16777216
sudo sysctl -w net.core.wmem_max=16777216
sudo sysctl -w net.ipv4.tcp_rmem="4096 87380 16777216"
sudo sysctl -w net.ipv4.tcp_wmem="4096 65536 16777216"
```

### 5.2 File Descriptor Limits
Support tens of thousands of simultaneous client sessions:
```bash
ulimit -n 65536
```

### 5.3 Binnacle Broker Configuration
Tune `binnacle.conf` for maximum I/O concurrency:
```ini
broker.max_connections = 50000
broker.receive_maximum = 1000
broker.max_offline_messages = 50000
storage.sync_on_write = false
storage.max_segment_size = 67108864
```
