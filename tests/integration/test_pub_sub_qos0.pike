// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/integration/test_pub_sub_qos0.pike
// Description: Integration test for end-to-end Publish and Subscribe at QoS 0.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("End-to-End Pub/Sub QoS 0");

    runner->test("Subscriber receives QoS 0 message published by separate connection", lambda() {
        int run = 1;
        object broker = Binnacle.Engine.BrokerServer();
        broker->start();

        int port = 11884;
        object tcp = Binnacle.Transport.TcpServer(broker, "127.0.0.1", port);
        runner->assert_true(tcp->start());

        Thread.Thread(lambda() {
            while (run) {
                Pike.DefaultBackend(0.02);
            }
        });

        sleep(1);

        // 1. Connect Subscriber
        Stdio.File sub_sock = Stdio.File();
        runner->assert_true(sub_sock->connect("127.0.0.1", port));
        object cp_sub = Binnacle.Protocol.ConnectPacket("sub-client-01");
        sub_sock->write(cp_sub->encode());
        sleep(1);
        string connack1 = sub_sock->read(64, 1);
        runner->assert_true(sizeof(connack1) > 0);

        // Subscribe to telemetry/temperature
        object sub_pkt = Binnacle.Protocol.SubscribePacket(1);
        sub_pkt->add_subscription("telemetry/temperature", 0);
        sub_sock->write(sub_pkt->encode());
        sleep(1);
        string suback = sub_sock->read(64, 1);
        runner->assert_true(sizeof(suback) > 0);
        runner->assert_equal(suback[0] >> 4, Binnacle.Constants.PACKET_SUBACK);

        // 2. Connect Publisher
        Stdio.File pub_sock = Stdio.File();
        runner->assert_true(pub_sock->connect("127.0.0.1", port));
        object cp_pub = Binnacle.Protocol.ConnectPacket("pub-client-01");
        pub_sock->write(cp_pub->encode());
        sleep(1);
        string connack2 = pub_sock->read(64, 1);
        runner->assert_true(sizeof(connack2) > 0);

        // Publish message
        string payload = "{\"temp_c\": 24.5}";
        object pub = Binnacle.Protocol.PublishPacket("telemetry/temperature", payload, 0);
        pub_sock->write(pub->encode());
        sleep(1);

        // 3. Verify Subscriber received the PUBLISH
        string delivered_raw = sub_sock->read(256, 1);
        runner->assert_true(sizeof(delivered_raw) > 0, "Subscriber must receive delivered message");
        array res = Binnacle.Protocol.PacketParser.parse_packet(delivered_raw);
        runner->assert_not_null(res, "Delivered packet must parse cleanly");
        object delivered_pkt = res[0];
        runner->assert_equal(delivered_pkt->packet_type, Binnacle.Constants.PACKET_PUBLISH);
        runner->assert_equal(delivered_pkt->topic, "telemetry/temperature");
        runner->assert_equal(delivered_pkt->payload, payload);
        runner->assert_equal(delivered_pkt->qos, 0);

        // Cleanup
        sub_sock->close();
        pub_sock->close();
        run = 0;
        tcp->stop();
        broker->stop();
    });

    return runner->report();
}
