// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// REST API Request Router and Handler

object broker;

void create(object _broker) {
    broker = _broker;
}

//! Dispatch an incoming HTTP request to the matching API route.
//! Returns ({ int status_code, string content_type, string body }).
array(mixed) dispatch(string method, string path, mapping headers, string body) {
    // 1. Root and Dashboard
    if (path == "/" || path == "/dashboard") {
        string html = .DashboardAssets.get_dashboard_html();
        return ({ 200, "text/html; charset=utf-8", html });
    }

    // 2. Health Endpoint
    if (path == "/health" || path == "/api/v1/health" || path == "/api/v1/status") {
        mapping resp = ([
            "status": "ok",
            "version": Binnacle.version(),
            "uptime_seconds": time() - (broker->sys_topics ? broker->sys_topics->start_time : time())
        ]);
        return ({ 200, "application/json", Standards.JSON.encode(resp) });
    }

    // 3. Prometheus / OpenMetrics Telemetry
    if (path == "/metrics" || path == "/api/v1/metrics") {
        string metrics_txt = Binnacle.Metrics.PrometheusExporter.export_openmetrics(broker, broker->metrics_registry);
        return ({ 200, "text/plain; version=0.0.4; charset=utf-8", metrics_txt });
    }

    // 4. Broker Stats API
    if (path == "/api/v1/stats") {
        mapping stats = ([
            "node_id": broker->config ? broker->config->get_string("node_id", "binnacle-01") : "binnacle-01",
            "version": Binnacle.version(),
            "clients_connected": broker->get_connected_clients_count(),
            "clients_total": broker->get_total_clients_count(),
            "subscriptions_count": broker->get_subscriptions_count(),
            "retained_count": broker->get_retained_count(),
            "messages_received": broker->get_messages_received_count(),
            "messages_sent": broker->get_messages_sent_count(),
            "bytes_received": broker->get_bytes_received_count(),
            "bytes_sent": broker->get_bytes_sent_count()
        ]);
        return ({ 200, "application/json", Standards.JSON.encode(stats) });
    }

    // 5. Retained Topics API
    if (path == "/api/v1/topics") {
        array(object) retained = broker->retained_store->to_array();
        array(mapping) list = ({});
        foreach (retained, object rm) {
            string preview = rm->payload;
            if (sizeof(preview) > 64) preview = preview[0..63] + "...";
            list += ({ ([
                "topic": rm->topic,
                "qos": rm->qos,
                "retained": 1,
                "payload_preview": preview
            ]) });
        }
        return ({ 200, "application/json", Standards.JSON.encode(list) });
    }

    // 6. REST Ingestion Bridge: POST /api/v1/publish
    if (path == "/api/v1/publish" && method == "POST") {
        mixed parsed = 0;
        catch {
            parsed = Standards.JSON.decode(body);
        };
        if (!parsed || !mappingp(parsed) || !has_index(parsed, "topic") || !has_index(parsed, "payload")) {
            return ({ 400, "application/json", "{\"error\":\"Missing topic or payload in JSON request\"}" });
        }

        string topic = parsed->topic;
        string payload = stringp(parsed->payload) ? parsed->payload : Standards.JSON.encode(parsed->payload);
        int qos = parsed->qos || 0;
        int retain = parsed->retain ? 1 : 0;

        broker->internal_publish(topic, payload, qos, retain);
        return ({ 200, "application/json", "{\"status\":\"published\"}" });
    }

    return ({ 404, "application/json", "{\"error\":\"Not Found\"}" });
}
