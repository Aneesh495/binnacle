// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Embedded Dashboard Single-Page Web Application

string get_dashboard_html() {
    return #"<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>Binnacle // Telemetry Fabric Console</title>
  <style>
    :root {
      --bg: #090d16;
      --card-bg: #111827;
      --border: #1f2937;
      --text: #f3f4f6;
      --text-muted: #9ca3af;
      --accent: #3b82f6;
      --accent-glow: rgba(59, 130, 246, 0.2);
      --success: #10b981;
      --warn: #f59e0b;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", Roboto, monospace;
      padding: 24px;
      line-height: 1.5;
    }
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 32px;
      padding-bottom: 16px;
      border-bottom: 1px solid var(--border);
    }
    .logo {
      font-size: 24px;
      font-weight: 700;
      letter-spacing: -0.5px;
      color: var(--text);
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .badge {
      font-size: 11px;
      text-transform: uppercase;
      background: var(--accent-glow);
      color: var(--accent);
      border: 1px solid var(--accent);
      padding: 2px 8px;
      border-radius: 4px;
      font-weight: 600;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 20px;
    }
    .card-title {
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      color: var(--text-muted);
      margin-bottom: 8px;
    }
    .card-value {
      font-size: 28px;
      font-weight: 700;
      color: var(--text);
    }
    .section-title {
      font-size: 16px;
      font-weight: 600;
      margin-bottom: 16px;
      color: var(--text);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }
    th, td {
      text-align: left;
      padding: 10px 12px;
      border-bottom: 1px solid var(--border);
    }
    th {
      color: var(--text-muted);
      font-weight: 600;
      text-transform: uppercase;
      font-size: 11px;
    }
    .status-dot {
      display: inline-block;
      width: 8px;
      height: 8px;
      background: var(--success);
      border-radius: 50%;
      margin-right: 6px;
    }
  </style>
</head>
<body>
  <header>
    <div class=\"logo\">
      <span>BINNACLE</span>
      <span class=\"badge\">MQTT 5.0 ENGINE</span>
    </div>
    <div style=\"font-size: 13px; color: var(--text-muted);\">
      <span class=\"status-dot\"></span>Node Active: <strong id=\"node-id\" style=\"color:var(--text);\">binnacle-01</strong>
    </div>
  </header>

  <div class=\"grid\">
    <div class=\"card\">
      <div class=\"card-title\">Active Connections</div>
      <div class=\"card-value\" id=\"val-clients\">0</div>
    </div>
    <div class=\"card\">
      <div class=\"card-title\">Messages Received</div>
      <div class=\"card-value\" id=\"val-rx\">0</div>
    </div>
    <div class=\"card\">
      <div class=\"card-title\">Messages Dispatched</div>
      <div class=\"card-value\" id=\"val-tx\">0</div>
    </div>
    <div class=\"card\">
      <div class=\"card-title\">Retained Topics</div>
      <div class=\"card-value\" id=\"val-retained\">0</div>
    </div>
  </div>

  <div class=\"grid\" style=\"grid-template-columns: 2fr 1fr;\">
    <div class=\"card\">
      <div class=\"section-title\">Recent Active Topics</div>
      <table>
        <thead>
          <tr>
            <th>Topic</th>
            <th>QoS</th>
            <th>Retained</th>
            <th>Payload Preview</th>
          </tr>
        </thead>
        <tbody id=\"topics-table\">
          <tr><td colspan=\"4\" style=\"color: var(--text-muted);\">Loading topic state...</td></tr>
        </tbody>
      </table>
    </div>

    <div class=\"card\">
      <div class=\"section-title\">Broker Metadata</div>
      <div style=\"font-size: 13px; display: flex; flex-direction: column; gap: 10px;\">
        <div><span style=\"color:var(--text-muted);\">Engine:</span> Pike v8.0</div>
        <div><span style=\"color:var(--text-muted);\">Protocol:</span> OASIS MQTT 5.0 / 3.1.1</div>
        <div><span style=\"color:var(--text-muted);\">Storage:</span> Append-Only Segment WAL</div>
        <div><span style=\"color:var(--text-muted);\">Metrics:</span> <a href=\"/metrics\" style=\"color:var(--accent);\">/metrics</a></div>
      </div>
    </div>
  </div>

  <script>
    async function refresh() {
      try {
        const res = await fetch('/api/v1/stats');
        const data = await res.json();
        document.getElementById('val-clients').innerText = data.clients_connected || 0;
        document.getElementById('val-rx').innerText = data.messages_received || 0;
        document.getElementById('val-tx').innerText = data.messages_sent || 0;
        document.getElementById('val-retained').innerText = data.retained_count || 0;
        document.getElementById('node-id').innerText = data.node_id || 'binnacle';

        const topRes = await fetch('/api/v1/topics');
        const topics = await topRes.json();
        const tbody = document.getElementById('topics-table');
        if (topics && topics.length > 0) {
          tbody.innerHTML = topics.map(t => `
            <tr>
              <td><code>${t.topic}</code></td>
              <td>QoS ${t.qos}</td>
              <td>${t.retained ? 'Yes' : 'No'}</td>
              <td><span style=\"color:var(--text-muted);\">${t.payload_preview}</span></td>
            </tr>
          `).join('');
        }
      } catch (e) {
        console.error(e);
      }
    }
    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>
";
}
