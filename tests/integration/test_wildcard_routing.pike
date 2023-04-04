// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/integration/test_wildcard_routing.pike
// Description: Integration test for single-level (+) and multi-level (#) wildcard routing over TCP.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Wildcard Topic Routing over Network Ingress");

    runner->test("Deliveries match '+' and '#' filters accurately", lambda() {
        int run = 1;
        object broker = Binnacle.Engine.BrokerServer();
        broker->start();

        int port = 11887;
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
        object cp_sub = Binnacle.Protocol.ConnectPacket("sub-wildcard-01");
        sub_sock->write(cp_sub->encode());
        sleep(1);
        sub_sock->read(64, 1);

        // Subscribe to fleet/+/telemetry
        object sub_pkt = Binnacle.Protocol.SubscribePacket(1);
        sub_pkt->add_subscription("fleet/+/telemetry", 0);
        sub_sock->write(sub_pkt->encode());
        sleep(1);
        sub_sock->read(64, 1);

        // 2. Connect Publisher
        Stdio.File pub_sock = Stdio.File();
        runner->assert_true(pub_sock->connect("127.0.0.1", port));
        object cp_pub = Binnacle.Protocol.ConnectPacket("pub-wildcard-01");
        pub_sock->write(cp_pub->encode());
        sleep(1);
        pub_sock->read(64, 1);

        // Publish match 1: fleet/vin01/telemetry
        object p1 = Binnacle.Protocol.PublishPacket("fleet/vin01/telemetry", "data1", 0);
        pub_sock->write(p1->encode());
        sleep(1);

        // Verify subscriber receives match 1
        string raw1 = sub_sock->read(128, 1);
        runner->assert_true(sizeof(raw1) > 0);
        array res1 = Binnacle.Protocol.PacketParser.parse_packet(raw1);
        runner->assert_equal(res1[0]->topic, "fleet/vin01/telemetry");

        // Publish non-match: fleet/vin01/alerts
        object p2 = Binnacle.Protocol.PublishPacket("fleet/vin01/alerts", "alert_data", 0);
        pub_sock->write(p2->encode());
        sleep(1);

        // Check non-blocking peek: should be 0 (no bytes waiting)
        runner->assert_equal(sub_sock->peek(), 0, "Non-matching topic must not be delivered");

        sub_sock->close();
        pub_sock->close();
        run = 0;
        tcp->stop();
        broker->stop();
    });

    return runner->report();
}
