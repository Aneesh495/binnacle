// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/unit/test_config_parser.pike
// Description: Unit tests for declarative configuration file parsing and typed getters.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Declarative Configuration Parser");

    runner->test("Parse declarative config text and typed getters", lambda() {
        string conf_text =
            "# Binnacle Test Config\n" +
            "broker.node_id = \"node-edge-01\"\n" +
            "broker.tcp_port = 1883\n" +
            "broker.tls_enabled = false\n" +
            "broker.auth_required = true\n" +
            "broker.max_connections = 50000\n" +
            "cluster.peers = \"10.0.0.1:1883, 10.0.0.2:1883, 10.0.0.3:1883\"\n";

        object cfg = Binnacle.Utils.ConfigParser.parse_string(conf_text);
        runner->assert_not_null(cfg);

        runner->assert_equal(cfg->get_string("broker.node_id"), "node-edge-01");
        runner->assert_equal(cfg->get_int("broker.tcp_port"), 1883);
        runner->assert_equal(cfg->get_bool("broker.tls_enabled"), 0);
        runner->assert_equal(cfg->get_bool("broker.auth_required"), 1);
        runner->assert_equal(cfg->get_int("broker.max_connections"), 50000);

        array peers = cfg->get_string_list("cluster.peers");
        runner->assert_equal(sizeof(peers), 3);
        runner->assert_equal(peers[0], "10.0.0.1:1883");
        runner->assert_equal(peers[1], "10.0.0.2:1883");
        runner->assert_equal(peers[2], "10.0.0.3:1883");
    });

    runner->test("Fallback default values for absent keys", lambda() {
        object cfg = Binnacle.Utils.ConfigParser.parse_string("");
        runner->assert_equal(cfg->get_string("missing.str", "default_str"), "default_str");
        runner->assert_equal(cfg->get_int("missing.int", 999), 999);
        runner->assert_equal(cfg->get_bool("missing.bool", 1), 1);
    });

    return runner->report();
}
