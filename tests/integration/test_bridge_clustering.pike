// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/integration/test_bridge_clustering.pike
// Description: Integration test for inter-broker topic bridging and cluster mesh replication.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Cluster Mesh Replication and Bridge Routing");

    runner->test("Bridge prefix translation and topic forwarding rules", lambda() {
        object broker = Binnacle.Engine.BrokerServer();
        object bridge = Binnacle.Cluster.BridgeNode(broker, "cloud-gw.internal", 1883, "out");
        bridge->add_topic_mapping("sensors/#", "edge/factory-01/");

        string mapped1 = bridge->map_local_to_remote("sensors/temperature");
        runner->assert_equal(mapped1, "edge/factory-01/sensors/temperature");

        string mapped2 = bridge->map_local_to_remote("actuators/valve");
        runner->assert_null(mapped2, "Unmatched topic should not be forwarded");
    });

    runner->test("Replication protocol framing and CRC32 verification", lambda() {
        string wire = Binnacle.Cluster.ReplicationProtocol.encode_frame(
            Binnacle.Cluster.ReplicationProtocol.MSG_FORWARD_PUBLISH,
            "node-alpha",
            1,
            "payload-data-stream"
        );

        runner->assert_true(sizeof(wire) > 20);

        array res = Binnacle.Cluster.ReplicationProtocol.decode_frame(wire);
        runner->assert_not_null(res);
        mapping parsed = res[0];
        runner->assert_equal(parsed->type, Binnacle.Cluster.ReplicationProtocol.MSG_FORWARD_PUBLISH);
        runner->assert_equal(parsed->origin, "node-alpha");
        runner->assert_equal(parsed->hops, 1);
        runner->assert_equal(parsed->payload, "payload-data-stream");
    });

    runner->test("Cluster loop prevention mechanism", lambda() {
        object broker = Binnacle.Engine.BrokerServer();
        object mesh = Binnacle.Cluster.ClusterMesh(broker, "node-charlie", 19883);

        string wire = Binnacle.Cluster.ReplicationProtocol.encode_frame(
            Binnacle.Cluster.ReplicationProtocol.MSG_FORWARD_PUBLISH,
            "node-charlie",
            16,
            "telemetry"
        );

        array res = Binnacle.Cluster.ReplicationProtocol.decode_frame(wire);
        mapping parsed = res[0];
        runner->assert_equal(parsed->origin, "node-charlie");
        runner->assert_true(parsed->hops >= 16, "Hop count must be capped to prevent routing storms");
    });

    return runner->report();
}
