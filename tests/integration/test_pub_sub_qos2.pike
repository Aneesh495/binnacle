// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/integration/test_pub_sub_qos2.pike
// Description: Integration test for QoS 2 exactly-once four-step delivery handshake.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("End-to-End Pub/Sub QoS 2 Exactly-Once Handshake");

    runner->test("QoS 2 four-step handshake (PUBLISH->PUBREC->PUBREL->PUBCOMP)", lambda() {
        int run = 1;
        object broker = Binnacle.Engine.BrokerServer();
        broker->start();

        int port = 11886;
        object tcp = Binnacle.Transport.TcpServer(broker, "127.0.0.1", port);
        runner->assert_true(tcp->start());

        Thread.Thread(lambda() {
            while (run) {
                Pike.DefaultBackend(0.02);
            }
        });

        sleep(1);

        // Connect Publisher
        Stdio.File pub_sock = Stdio.File();
        runner->assert_true(pub_sock->connect("127.0.0.1", port));
        object cp_pub = Binnacle.Protocol.ConnectPacket("pub-qos2-01");
        pub_sock->write(cp_pub->encode());
        sleep(1);
        pub_sock->read(64, 1);

        // 1. Publish QoS 2 message (Packet ID = 77)
        object pub = Binnacle.Protocol.PublishPacket("finance/trades", "{\"trade\": 100}", 2, 0, 0, 77);
        pub_sock->write(pub->encode());
        sleep(1);

        // 2. Broker responds with PUBREC
        string rec_raw = pub_sock->read(32, 1);
        runner->assert_true(sizeof(rec_raw) >= 2);
        array rec_res = Binnacle.Protocol.PacketParser.parse_packet(rec_raw);
        runner->assert_not_null(rec_res);
        runner->assert_equal(rec_res[0]->packet_type, Binnacle.Constants.PACKET_PUBREC);
        runner->assert_equal(rec_res[0]->packet_id, 77);

        // 3. Publisher sends PUBREL
        object pubrel = Binnacle.Protocol.PubRelPacket(77, Binnacle.Constants.RC_SUCCESS);
        pub_sock->write(pubrel->encode());
        sleep(1);

        // 4. Broker responds with PUBCOMP
        string comp_raw = pub_sock->read(32, 1);
        runner->assert_true(sizeof(comp_raw) >= 2);
        array comp_res = Binnacle.Protocol.PacketParser.parse_packet(comp_raw);
        runner->assert_not_null(comp_res);
        runner->assert_equal(comp_res[0]->packet_type, Binnacle.Constants.PACKET_PUBCOMP);
        runner->assert_equal(comp_res[0]->packet_id, 77);

        pub_sock->close();
        run = 0;
        tcp->stop();
        broker->stop();
    });

    return runner->report();
}
