// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Prometheus and OpenMetrics Text Exposition Formatter

//! Export broker metrics registry to standard Prometheus scrape format.
string export_openmetrics(object broker, object|void registry) {
    string out = "";

    out += "# HELP binnacle_uptime_seconds Total seconds broker has been running\n";
    out += "# TYPE binnacle_uptime_seconds gauge\n";
    out += sprintf("binnacle_uptime_seconds %d\n\n", time() - (broker->sys_topics ? broker->sys_topics->start_time : time()));

    out += "# HELP binnacle_connected_clients Number of active live client connections\n";
    out += "# TYPE binnacle_connected_clients gauge\n";
    out += sprintf("binnacle_connected_clients %d\n\n", broker->get_connected_clients_count());

    out += "# HELP binnacle_total_sessions Number of registered sessions in state store\n";
    out += "# TYPE binnacle_total_sessions gauge\n";
    out += sprintf("binnacle_total_sessions %d\n\n", broker->get_total_clients_count());

    out += "# HELP binnacle_subscriptions_count Total registered topic subscriptions\n";
    out += "# TYPE binnacle_subscriptions_count gauge\n";
    out += sprintf("binnacle_subscriptions_count %d\n\n", broker->get_subscriptions_count());

    out += "# HELP binnacle_retained_messages_count Total retained messages stored in broker\n";
    out += "# TYPE binnacle_retained_messages_count gauge\n";
    out += sprintf("binnacle_retained_messages_count %d\n\n", broker->get_retained_count());

    out += "# HELP binnacle_messages_received_total Cumulative messages received by broker\n";
    out += "# TYPE binnacle_messages_received_total counter\n";
    out += sprintf("binnacle_messages_received_total %d\n\n", broker->get_messages_received_count());

    out += "# HELP binnacle_messages_sent_total Cumulative messages dispatched to subscribers\n";
    out += "# TYPE binnacle_messages_sent_total counter\n";
    out += sprintf("binnacle_messages_sent_total %d\n\n", broker->get_messages_sent_count());

    out += "# HELP binnacle_bytes_received_total Cumulative network bytes received\n";
    out += "# TYPE binnacle_bytes_received_total counter\n";
    out += sprintf("binnacle_bytes_received_total %d\n\n", broker->get_bytes_received_count());

    out += "# HELP binnacle_bytes_sent_total Cumulative network bytes transmitted\n";
    out += "# TYPE binnacle_bytes_sent_total counter\n";
    out += sprintf("binnacle_bytes_sent_total %d\n\n", broker->get_bytes_sent_count());

    if (registry) {
        mapping metrics = registry->get_all_metrics();

        foreach (indices(metrics->counters), string c_name) {
            out += sprintf("# TYPE %s counter\n%s %d\n\n", c_name, c_name, metrics->counters[c_name]);
        }

        foreach (indices(metrics->gauges), string g_name) {
            out += sprintf("# TYPE %s gauge\n%s %f\n\n", g_name, g_name, metrics->gauges[g_name]);
        }

        foreach (indices(metrics->histograms), string h_name) {
            mapping summary = metrics->histograms[h_name];
            out += sprintf("# TYPE %s summary\n", h_name);
            out += sprintf("%s{quantile=\"0.50\"} %f\n", h_name, summary->p50_us);
            out += sprintf("%s{quantile=\"0.90\"} %f\n", h_name, summary->p90_us);
            out += sprintf("%s{quantile=\"0.95\"} %f\n", h_name, summary->p95_us);
            out += sprintf("%s{quantile=\"0.99\"} %f\n", h_name, summary->p99_us);
            out += sprintf("%s_count %f\n", h_name, summary->count);
            out += sprintf("%s_max %f\n\n", h_name, summary->max_us);
        }
    }

    return out;
}
