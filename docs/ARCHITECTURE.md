# Binnacle System Architecture

## 1. Overview

Binnacle is a distributed, high-throughput MQTT 5.0 telemetry broker, streaming event engine, and edge messaging hypervisor designed in Pike. It provides wire-compatible OASIS MQTT 5.0 and 3.1.1 ingestion, a hierarchical radix trie topic router, zero-copy QoS 0/1/2 delivery pipelines, an append-only Write-Ahead Log (WAL) with point-in-time snapshots, embedded Complex Event Processing (CEP) rules with tumbling and sliding windows, and inter-broker mesh clustering.

```mermaid
graph TD
    subgraph NetworkIngress ["Network Ingress Layer"]
        TCP["TCP Listener (Port 1883)"]
        TLS["TLS Listener (Port 8883)"]
        WS["WebSocket Listener (Port 8083)"]
        HTTP["HTTP Management (Port 8080)"]
    end

    subgraph ActorLayer ["Actor Execution Layer"]
        ConnActor["Connection Actors"]
        Parser["Streaming Wire Deserializer"]
        Limiter["Token Bucket Rate Limiter"]
    end

    subgraph CoreEngine ["Broker Core Engine"]
        Router["Radix Topic Trie Router"]
        Sessions["Session Manager & Registry"]
        QoS["QoS 0/1/2 Handshake State Machines"]
        Window["Inflight Window Flow Control"]
        Retain["Retained Message Store"]
        Will["Last Will and Testament Manager"]
    end

    subgraph ProcessingStorage ["Processing & Storage Subsystems"]
        WAL["Write-Ahead Log (WAL) Segments"]
        Snap["Snapshot Compaction Manager"]
        CEP["Streaming Rules & Window Aggregators"]
        Mesh["Cluster Mesh Replication Engine"]
        Metrics["Prometheus & HDR Latency Histograms"]
    end

    TCP --> ConnActor
    TLS --> ConnActor
    WS --> ConnActor
    HTTP --> Metrics

    ConnActor --> Parser
    Parser --> Limiter
    Limiter --> CoreEngine

    Router --> CoreEngine
    CoreEngine --> WAL
    CoreEngine --> Snap
    CoreEngine --> CEP
    CoreEngine --> Mesh
    CoreEngine --> Metrics
```

---

## 2. Component Decomposition

### 2.1 Network Ingress Layer
The ingress layer handles non-blocking asynchronous socket multiplexing across plain TCP, TLS, and RFC 6455 WebSockets. Each incoming client connection is immediately bound to a lightweight `ConnectionActor` that encapsulates transport buffers, parser state, rate limits, and keepalive tracking.

### 2.2 Streaming Wire Deserializer
The protocol parser processes streaming TCP byte chunks without assuming packet boundary alignment. Variable-byte integers (1 to 4 bytes) are dynamically resolved to compute the packet's Remaining Length, after which the parser verifies fixed header flags and instantiates the appropriate packet representation.

### 2.3 Radix Topic Trie Router
The topic routing fabric uses a radix trie where tree branches correspond to topic levels delimited by `/`. Single-level (`+`) and multi-level (`#`) wildcards are evaluated with zero heap allocations for static paths. Subscriptions with the `NoLocal` option prevent messages from looping back to their original publisher.

### 2.4 State Management and Session Registry
Client state is governed by MQTT 5.0 CleanStart and Session Expiry Interval rules. When a client disconnects with an active Session Expiry Interval, the session transitions to an inactive state while preserving offline queues, inflight packet windows, and subscription trees. Expired sessions are reaped by an asynchronous background cleaner.

### 2.5 Durability Subsystem (WAL)
Every state mutation (retained message updates, persistent session creation, subscription changes) is appended to an active 64 MB segment file with an 8-byte monotonic Log Sequence Number (LSN) and a hardware-optimized IEEE 802.3 CRC32 checksum. Crash recovery sequentially replays segment files and flags torn writes.

---

## 3. Connection Actor State Machine

```mermaid
stateDiagram-v2
    [*] --> Connecting : Socket Accepted
    Connecting --> Handshaking : Read Fixed Header
    Handshaking --> Authenticating : Parse CONNECT Packet
    Authenticating --> Active : Auth OK & Session Established
    Authenticating --> Closed : Auth Failed / Protocol Error
    Active --> Throttled : Token Bucket Depleted
    Throttled --> Active : Tokens Replenished
    Active --> Disconnecting : DISCONNECT Received
    Active --> AbnormalTermination : Socket RST / KeepAlive Timeout
    AbnormalTermination --> DispatchWill : Will Registered
    AbnormalTermination --> Closed : No Will
    DispatchWill --> Closed
    Disconnecting --> Closed
    Closed --> [*]
```

---

## 4. Concurrency Model

Binnacle leverages Pike's hybrid concurrency model:
1. **Event-Driven I/O Multiplexing:** Socket read/write events are scheduled via Pike's native backend loop.
2. **Fine-Grained Mutex Locking:** Shared subsystem resources (Topic Trie, Session Registry, Inflight Windows, Metrics) utilize granular `Thread.Mutex` instances rather than a global broker lock. This ensures concurrent client throughput across multiple CPU cores without contention bottlenecks.
3. **Actor Isolation:** Each client connection operates as an independent actor maintaining its own token bucket shaper and packet identifier leasing pool.
