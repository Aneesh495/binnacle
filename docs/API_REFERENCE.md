# Binnacle API and Configuration Reference

## 1. HTTP REST Management Endpoints

Binnacle embeds an administrative REST API running on port `8080` (or configured `admin.http_port`).

### 1.1 `GET /health` or `GET /api/v1/health`
Returns broker operational health and system uptime.

**Response (200 OK):**
```json
{
  "status": "ok",
  "version": "1.0.0",
  "uptime_seconds": 1842
}
```

---

### 1.2 `GET /api/v1/stats`
Returns aggregated throughput and resource metrics across all broker subsystems.

**Response (200 OK):**
```json
{
  "node_id": "binnacle-01",
  "version": "1.0.0",
  "clients_connected": 42,
  "clients_total": 128,
  "subscriptions_count": 310,
  "retained_count": 18,
  "messages_received": 524100,
  "messages_sent": 519800,
  "bytes_received": 67084800,
  "bytes_sent": 66534400
}
```

---

### 1.3 `GET /api/v1/clients`
Returns a list of currently registered sessions and connection statuses.

**Response (200 OK):**
```json
[
  {
    "client_id": "fleet-vehicle-01",
    "is_connected": true,
    "clean_start": true,
    "session_expiry": 3600,
    "subscriptions": ["fleet/+/telematics", "alerts/#"],
    "connected_at": 1730592000
  }
]
```

---

### 1.4 `GET /api/v1/topics`
Lists active topic filters and retained message counts.

---

### 1.5 `POST /api/v1/publish`
Allows external microservices to inject messages into the broker routing trie over HTTP.

**Request Body:**
```json
{
  "topic": "alerts/facility/critical",
  "payload": "{\"alert\": \"High Temperature Detected\"}",
  "qos": 1,
  "retain": false
}
```

**Response (200 OK):**
```json
{
  "status": "published",
  "topic": "alerts/facility/critical",
  "dispatched_to": 4
}
```

---

### 1.6 `GET /metrics` or `GET /api/v1/metrics`
Exposes system telemetry in OpenMetrics and Prometheus text exposition format.

---

## 2. Prometheus Metric Dictionary

| Metric Name | Type | Description |
|:---|:---|:---|
| `binnacle_uptime_seconds` | Gauge | Total seconds broker daemon has been active |
| `binnacle_connected_clients` | Gauge | Number of live TCP/TLS/WebSocket client connections |
| `binnacle_total_sessions` | Gauge | Total persistent sessions in the registry |
| `binnacle_subscriptions_count` | Gauge | Number of active subscriptions in the Radix Trie |
| `binnacle_retained_messages_count` | Gauge | Number of messages currently retained |
| `binnacle_messages_received_total` | Counter | Cumulative MQTT messages ingested by broker |
| `binnacle_messages_sent_total` | Counter | Cumulative MQTT messages dispatched to clients |
| `binnacle_bytes_received_total` | Counter | Cumulative raw network bytes received |
| `binnacle_bytes_sent_total` | Counter | Cumulative raw network bytes transmitted |

---

## 3. Configuration Directives Dictionary

Configuration options are specified in key-value format in `binnacle.conf`.

| Directive | Type | Default | Description |
|:---|:---|:---|:---|
| `broker.node_id` | String | `"binnacle-node-01"` | Unique node identifier within the cluster |
| `broker.tcp_port` | Integer | `1883` | TCP ingress port |
| `broker.tcp_bind` | String | `"0.0.0.0"` | Network interface bind address |
| `broker.tls_enabled` | Boolean | `false` | Enable TLS encryption |
| `broker.tls_port` | Integer | `8883` | TLS ingress port |
| `broker.ws_enabled` | Boolean | `true` | Enable WebSocket ingress |
| `broker.ws_port` | Integer | `8083` | WebSocket port |
| `admin.http_port` | Integer | `8080` | REST API and Web Dashboard port |
| `broker.max_connections` | Integer | `100000` | Maximum simultaneous open connections |
| `broker.receive_maximum` | Integer | `65535` | Inflight window maximum per connection |
| `broker.max_offline_messages` | Integer | `10000` | Maximum queue size for offline clients |
| `broker.offline_queue_strategy`| String | `"drop_oldest"` | Eviction policy (`"drop_oldest"` or `"drop_newest"`) |
| `broker.allow_anonymous` | Boolean | `true` | Allow clients without credentials |
| `storage.wal_dir` | String | `"./data/wal"` | Storage directory for WAL segment files |
| `storage.sync_on_write` | Boolean | `false` | Force fsync on every append |
| `storage.max_segment_size` | Integer | `67108864` | Segment file size threshold (64 MB) |
| `cluster.mesh_port` | Integer | `9883` | Inter-broker replication cluster port |
| `cluster.peers` | String List | `""` | Comma-separated list of peer host:port addresses |
