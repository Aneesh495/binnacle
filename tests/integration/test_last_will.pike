// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/integration/test_last_will.pike
// Description: Integration test for Last Will and Testament (LWT) triggered upon abnormal disconnect.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Last Will and Testament (LWT) Abnormal Disconnect");

    runner->test("Broker publishes Will Message upon unexpected connection termination", lambda() {
        int run = 1;
        object broker = Binnacle.Engine.BrokerServer();
        broker->start();

        int port = 11889;
        object tcp = Binnacle.Transport.TcpServer(broker, "127.0.0.1", port);
        runner->assert_true(tcp->start());

        Thread.Thread(lambda() {
            while (run) {
                Pike.DefaultBackend(0.02);
            }
        });

        sleep(1);

        // 1. Connect Subscriber monitoring node status
        Stdio.File sub_sock = Stdio.File();
        runner->assert_true(sub_sock->connect("127.0.0.1", port));
        object cp_sub = Binnacle.Protocol.ConnectPacket("monitor-client-01");
        sub_sock->write(cp_sub->encode());
        sleep(1);
        sub_sock->read(64, 1);

        object sub_pkt = Binnacle.Protocol.SubscribePacket(1);
        sub_pkt->add_subscription("status/worker01/state", 0);
        sub_sock->write(sub_pkt->encode());
        sleep(1);
        sub_sock->read(64, 1);

        // 2. Connect Worker with Last Will configured
        Stdio.File worker_sock = Stdio.File();
        runner->assert_true(worker_sock->connect("127.0.0.1", port));
        object cp_worker = Binnacle.Protocol.ConnectPacket("worker-node-01");
        cp_worker->set_will("status/worker01/state", "offline-unclean", 0, 0);
        worker_sock->write(cp_worker->encode());
        sleep(1);
        worker_sock->read(64, 1);

        // 3. Worker socket dies abruptly without DISCONNECT packet
        worker_sock->close();
        sleep(1);

        // 4. Verify Monitor receives the Will Message
        string lwt_raw = sub_sock->read(256, 1);
        runner->assert_true(sizeof(lwt_raw) > 0, "Monitor must receive Will Message");
        array res = Binnacle.Protocol.PacketParser.parse_packet(lwt_raw);
        runner->assert_not_null(res);
        runner->assert_equal(res[0]->topic, "status/worker01/state");
        runner->assert_equal(res[0]->payload, "offline-unclean");

        sub_sock->close();
        run = 0;
        tcp->stop();
        broker->stop();
    });

    return runner->report();
}
