// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/unit/test_metrics_registry.pike
// Description: Unit tests for metrics registry, latency histograms, and Prometheus exporter.

class MockBroker {
    int get_connected_clients_count() { return 10; }
    int get_total_clients_count() { return 25; }
    int get_subscriptions_count() { return 45; }
    int get_retained_count() { return 5; }
    int get_messages_received_count() { return 1000; }
    int get_messages_sent_count() { return 950; }
    int get_bytes_received_count() { return 64000; }
    int get_bytes_sent_count() { return 61000; }
    object sys_topics = 0;
}

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Metrics Registry and OpenMetrics Exporter");

    runner->test("Counter increments and gauge sets", lambda() {
        object reg = Binnacle.Metrics.MetricsRegistry();
        reg->counter_inc("test_events_total");
        reg->counter_inc("test_events_total", 4);
        reg->gauge_set("active_memory_mb", 128.5);

        mapping all_m = reg->get_all_metrics();
        runner->assert_equal(all_m->counters->test_events_total, 5);
        runner->assert_equal(all_m->gauges->active_memory_mb, 128.5);
    });

    runner->test("LatencyHistogram percentiles computation", lambda() {
        object hist = Binnacle.Metrics.LatencyHistogram();
        // Record 100 observations: 1 to 100 us
        for (int i = 1; i <= 100; i++) {
            hist->record(i);
        }

        mapping s = hist->get_summary();
        runner->assert_equal(s->count, 100.0);
        runner->assert_true(s->p50_us >= 25.0 && s->p50_us <= 100.0);
        runner->assert_true(s->p90_us >= 50.0 && s->p90_us <= 100.0);
        runner->assert_true(s->p99_us >= 50.0 && s->p99_us <= 100.0);
    });

    runner->test("Prometheus OpenMetrics text exporter formatting", lambda() {
        object broker = MockBroker();
        object reg = Binnacle.Metrics.MetricsRegistry();
        reg->counter_inc("custom_packets_total", 77);

        string openmetrics = Binnacle.Metrics.PrometheusExporter.export_openmetrics(broker, reg);
        runner->assert_true(search(openmetrics, "binnacle_connected_clients 10") >= 0);
        runner->assert_true(search(openmetrics, "binnacle_messages_received_total 1000") >= 0);
        runner->assert_true(search(openmetrics, "custom_packets_total 77") >= 0);
    });

    return runner->report();
}
