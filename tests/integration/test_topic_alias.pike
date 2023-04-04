// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/integration/test_topic_alias.pike
// Description: Integration test for MQTT 5.0 Topic Alias bandwidth reduction mechanism.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("MQTT 5.0 Topic Alias Bandwidth Reduction");

    runner->test("Publisher establishes topic alias and transmits subsequent messages with empty topic", lambda() {
        int run = 1;
        object broker = Binnacle.Engine.BrokerServer();
        broker->start();

        int port = 11890;
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
        object cp_sub = Binnacle.Protocol.ConnectPacket("sub-alias-01");
        sub_sock->write(cp_sub->encode());
        sleep(1);
        sub_sock->read(64, 1);

        object sub_pkt = Binnacle.Protocol.SubscribePacket(1);
        sub_pkt->add_subscription("telemetry/speed", 0);
        sub_sock->write(sub_pkt->encode());
        sleep(1);
        sub_sock->read(64, 1);

        // 2. Connect Publisher
        Stdio.File pub_sock = Stdio.File();
        runner->assert_true(pub_sock->connect("127.0.0.1", port));
        object cp_pub = Binnacle.Protocol.ConnectPacket("pub-alias-01");
        pub_sock->write(cp_pub->encode());
        sleep(1);
        pub_sock->read(64, 1);

        // Message 1: registers alias 1 -> telemetry/speed
        mapping props1 = ([
            Binnacle.Constants.PROP_TOPIC_ALIAS: 1
        ]);
        object p1 = Binnacle.Protocol.PublishPacket("telemetry/speed", "100 kmh", 0, 0, 0, 0, props1);
        pub_sock->write(p1->encode());
        sleep(1);

        // Verify subscriber receives first message
        string r1 = sub_sock->read(256, 1);
        runner->assert_true(sizeof(r1) > 0);
        array res1 = Binnacle.Protocol.PacketParser.parse_packet(r1);
        runner->assert_equal(res1[0]->topic, "telemetry/speed");
        runner->assert_equal(res1[0]->payload, "100 kmh");

        // Message 2: empty topic string with Topic Alias = 1
        mapping props2 = ([
            Binnacle.Constants.PROP_TOPIC_ALIAS: 1
        ]);
        object p2 = Binnacle.Protocol.PublishPacket("", "105 kmh", 0, 0, 0, 0, props2);
        pub_sock->write(p2->encode());
        sleep(1);

        // Verify subscriber receives second message resolved to telemetry/speed
        string r2 = sub_sock->read(256, 1);
        runner->assert_true(sizeof(r2) > 0);
        array res2 = Binnacle.Protocol.PacketParser.parse_packet(r2);
        runner->assert_equal(res2[0]->topic, "telemetry/speed");
        runner->assert_equal(res2[0]->payload, "105 kmh");

        sub_sock->close();
        pub_sock->close();
        run = 0;
        tcp->stop();
        broker->stop();
    });

    return runner->report();
}
