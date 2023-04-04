// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/integration/test_pub_sub_qos1.pike
// Description: Integration test for end-to-end Publish and Subscribe with QoS 1 handshakes.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("End-to-End Pub/Sub QoS 1 with PUBACK");

    runner->test("QoS 1 message publication yields PUBACK and reliable delivery", lambda() {
        int run = 1;
        object broker = Binnacle.Engine.BrokerServer();
        broker->start();

        int port = 11885;
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
        object cp_sub = Binnacle.Protocol.ConnectPacket("sub-qos1-01");
        sub_sock->write(cp_sub->encode());
        sleep(1);
        sub_sock->read(64, 1);

        // Subscribe QoS 1
        object sub_pkt = Binnacle.Protocol.SubscribePacket(10);
        sub_pkt->add_subscription("orders/fulfillment", 1);
        sub_sock->write(sub_pkt->encode());
        sleep(1);
        sub_sock->read(64, 1);

        // 2. Connect Publisher
        Stdio.File pub_sock = Stdio.File();
        runner->assert_true(pub_sock->connect("127.0.0.1", port));
        object cp_pub = Binnacle.Protocol.ConnectPacket("pub-qos1-01");
        pub_sock->write(cp_pub->encode());
        sleep(1);
        pub_sock->read(64, 1);

        // Publish QoS 1 message with packet_id = 42
        string payload = "{\"order_id\": \"ord_9901\"}";
        object pub = Binnacle.Protocol.PublishPacket("orders/fulfillment", payload, 1, 0, 0, 42);
        pub_sock->write(pub->encode());
        sleep(1);

        // Verify publisher gets PUBACK
        string puback_raw = pub_sock->read(32, 1);
        runner->assert_true(sizeof(puback_raw) >= 2);
        array pa_res = Binnacle.Protocol.PacketParser.parse_packet(puback_raw);
        runner->assert_not_null(pa_res);
        runner->assert_equal(pa_res[0]->packet_type, Binnacle.Constants.PACKET_PUBACK);
        runner->assert_equal(pa_res[0]->packet_id, 42);

        // Verify subscriber gets message with QoS 1
        string delivered_raw = sub_sock->read(256, 1);
        runner->assert_true(sizeof(delivered_raw) >= 2);
        array deliv_res = Binnacle.Protocol.PacketParser.parse_packet(delivered_raw);
        runner->assert_not_null(deliv_res);
        runner->assert_equal(deliv_res[0]->topic, "orders/fulfillment");
        runner->assert_equal(deliv_res[0]->payload, payload);
        runner->assert_equal(deliv_res[0]->qos, 1);

        // Subscriber acknowledges by sending PUBACK
        object sub_ack = Binnacle.Protocol.PubAckPacket(deliv_res[0]->packet_id, Binnacle.Constants.RC_SUCCESS);
        sub_sock->write(sub_ack->encode());
        sleep(1);

        sub_sock->close();
        pub_sock->close();
        run = 0;
        tcp->stop();
        broker->stop();
    });

    return runner->report();
}
