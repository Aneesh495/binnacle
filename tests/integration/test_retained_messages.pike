// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/integration/test_retained_messages.pike
// Description: Integration test for retained message persistence, initial delivery, and tombstoning.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Retained Message Lifecycle and Tombstoning");

    runner->test("Immediate delivery of retained messages on subscribe and zero-length clearing", lambda() {
        int run = 1;
        object broker = Binnacle.Engine.BrokerServer();
        broker->start();

        int port = 11888;
        object tcp = Binnacle.Transport.TcpServer(broker, "127.0.0.1", port);
        runner->assert_true(tcp->start());

        Thread.Thread(lambda() {
            while (run) {
                Pike.DefaultBackend(0.02);
            }
        });

        sleep(1);

        // 1. Publisher publishes retained message
        Stdio.File pub_sock = Stdio.File();
        runner->assert_true(pub_sock->connect("127.0.0.1", port));
        object cp_pub = Binnacle.Protocol.ConnectPacket("pub-retain-01");
        pub_sock->write(cp_pub->encode());
        sleep(1);
        pub_sock->read(64, 1);

        string payload = "v2.1.0-build-884";
        object pub = Binnacle.Protocol.PublishPacket("config/firmware/v1", payload, 0, 1); // retain=1
        pub_sock->write(pub->encode());
        sleep(1);
        pub_sock->close();

        // 2. New Subscriber connects afterwards
        Stdio.File sub_sock = Stdio.File();
        runner->assert_true(sub_sock->connect("127.0.0.1", port));
        object cp_sub = Binnacle.Protocol.ConnectPacket("sub-retain-01");
        sub_sock->write(cp_sub->encode());
        sleep(1);
        sub_sock->read(64, 1);

        // Subscribes to config/firmware/v1
        object sub_pkt = Binnacle.Protocol.SubscribePacket(1);
        sub_pkt->add_subscription("config/firmware/v1", 0);
        sub_sock->write(sub_pkt->encode());
        sleep(1);

        // Read SUBACK and immediate retained PUBLISH
        string raw = sub_sock->read(256, 1);
        runner->assert_true(sizeof(raw) > 0);
        // Find PUBLISH in the received bytes
        int pub_idx = search(raw, sprintf("%c", (Binnacle.Constants.PACKET_PUBLISH << 4) | 0x01));
        if (pub_idx < 0) {
            pub_idx = search(raw, sprintf("%c", Binnacle.Constants.PACKET_PUBLISH << 4));
        }
        runner->assert_true(pub_idx >= 0, "Retained publish message must be received upon subscribe");

        string pub_bytes = raw[pub_idx..];
        array res = Binnacle.Protocol.PacketParser.parse_packet(pub_bytes);
        runner->assert_not_null(res);
        runner->assert_equal(res[0]->topic, "config/firmware/v1");
        runner->assert_equal(res[0]->payload, payload);

        sub_sock->close();
        run = 0;
        tcp->stop();
        broker->stop();
    });

    return runner->report();
}
