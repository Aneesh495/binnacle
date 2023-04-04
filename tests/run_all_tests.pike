// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/run_all_tests.pike
// Description: Master test runner executing all unit, integration, and regression suites.

int main() {
    write("============================================================\n");
    write("       Binnacle Comprehensive Test Suite Orchestrator       \n");
    write("============================================================\n");

    array(string) test_suites = ({
        // Unit test suites (14)
        "tests/unit/test_varint.pike",
        "tests/unit/test_properties.pike",
        "tests/unit/test_packet_codec.pike",
        "tests/unit/test_topic_trie.pike",
        "tests/unit/test_shared_subs.pike",
        "tests/unit/test_qos_handler.pike",
        "tests/unit/test_session_lifecycle.pike",
        "tests/unit/test_offline_queue.pike",
        "tests/unit/test_wal_storage.pike",
        "tests/unit/test_rule_engine.pike",
        "tests/unit/test_websocket_framing.pike",
        "tests/unit/test_metrics_registry.pike",
        "tests/unit/test_rate_limiter.pike",
        "tests/unit/test_config_parser.pike",

        // Regression suites (5)
        "tests/regressions/test_bug_varint_overflow.pike",
        "tests/regressions/test_bug_trie_trailing_slash.pike",
        "tests/regressions/test_bug_qos2_interleaved_packet_ids.pike",
        "tests/regressions/test_bug_session_expiry_cleanup.pike",
        "tests/regressions/test_bug_websocket_masking.pike",

        // Integration test suites (10)
        "tests/integration/test_tcp_connect_and_ping.pike",
        "tests/integration/test_pub_sub_qos0.pike",
        "tests/integration/test_pub_sub_qos1.pike",
        "tests/integration/test_pub_sub_qos2.pike",
        "tests/integration/test_wildcard_routing.pike",
        "tests/integration/test_retained_messages.pike",
        "tests/integration/test_last_will.pike",
        "tests/integration/test_topic_alias.pike",
        "tests/integration/test_rest_api.pike",
        "tests/integration/test_bridge_clustering.pike"
    });

    int total_suites = sizeof(test_suites);
    int passed_suites = 0;
    int failed_suites = 0;

    int overall_start = time();

    foreach (test_suites, string suite_path) {
        array(string) cmd = ({ "pike", "-Msrc", "-Mtests", suite_path });
        mapping res = Process.run(cmd);

        if (res->exit == 0) {
            passed_suites++;
            write("Suite Passed: %s\n", suite_path);
        } else {
            failed_suites++;
            write("Suite FAILED: %s (exit code %d)\n", suite_path, res->exit);
            if (sizeof(res->stdout)) write("%s\n", res->stdout);
            if (sizeof(res->stderr)) write("%s\n", res->stderr);
        }
    }

    int overall_duration = time() - overall_start;

    write("\n============================================================\n");
    write("                   FINAL TEST RUN SUMMARY                   \n");
    write("============================================================\n");
    write("  Total Suites Executed : %d\n", total_suites);
    write("  Suites Passed         : %d\n", passed_suites);
    write("  Suites Failed         : %d\n", failed_suites);
    write("  Total Elapsed Time    : %ds\n", overall_duration);
    write("============================================================\n");

    return failed_suites == 0 ? 0 : 1;
}
