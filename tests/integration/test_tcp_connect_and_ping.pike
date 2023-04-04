// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/integration/test_tcp_connect_and_ping.pike
// Description: Integration test for real TCP socket connect, PINGREQ/PINGRESP heartbeat, and clean disconnect.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("TCP Connect, Heartbeat Ping, and Disconnect");

    runner->test("Full TCP socket handshake, PING heartbeat, and DISCONNECT", lambda() {
        int run = 1;
        object broker = Binnacle.Engine.BrokerServer();
        broker->start();

        int port = 11883;
        object tcp = Binnacle.Transport.TcpServer(broker, "127.0.0.1", port);
        runner->assert_true(tcp->start(), "TCP server must bind successfully");

        // Run Pike event loop in background thread
        Thread.Thread(lambda() {
            while (run) {
                Pike.DefaultBackend(0.02);
            }
        });

        sleep(1);

        Stdio.File client = Stdio.File();
        int connected = client->connect("127.0.0.1", port);
        runner->assert_true(connected, "Client socket should connect to broker");

        // 1. Send CONNECT
        object cp = Binnacle.Protocol.ConnectPacket("tcp-ping-client-01");
        client->write(cp->encode());
        sleep(1);

        // Read CONNACK
        string raw_connack = client->read(64, 1);
        runner->assert_true(sizeof(raw_connack) >= 2, "CONNACK must be received");
        int pkt_type = raw_connack[0] >> 4;
        runner->assert_equal(pkt_type, Binnacle.Constants.PACKET_CONNACK, "Packet must be CONNACK");

        // 2. Send PINGREQ
        object ping = Binnacle.Protocol.PingReqPacket();
        client->write(ping->encode());
        sleep(1);

        // Read PINGRESP
        string raw_pingresp = client->read(2, 1);
        runner->assert_equal(sizeof(raw_pingresp), 2, "PINGRESP must be 2 bytes");
        runner->assert_equal(raw_pingresp[0] >> 4, Binnacle.Constants.PACKET_PINGRESP);
        runner->assert_equal(raw_pingresp[1], 0);

        // 3. Send DISCONNECT
        object disc = Binnacle.Protocol.DisconnectPacket(Binnacle.Constants.RC_NORMAL_DISCONNECTION);
        client->write(disc->encode());
        sleep(1);

        client->close();
        run = 0;
        tcp->stop();
        broker->stop();
    });

    return runner->report();
}
