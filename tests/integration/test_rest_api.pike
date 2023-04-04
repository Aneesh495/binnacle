// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/integration/test_rest_api.pike
// Description: Integration test for HTTP REST management endpoints and metrics exporter.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("HTTP REST Management API and Metrics Server");

    runner->test("REST endpoints /api/v1/status and /api/v1/metrics respond accurately", lambda() {
        int run = 1;
        object broker = Binnacle.Engine.BrokerServer();
        broker->start();

        int port = 18080;
        object http = Binnacle.Admin.HttpServer(broker, port, "127.0.0.1");
        runner->assert_true(http->start());

        Thread.Thread(lambda() {
            while (run) {
                Pike.DefaultBackend(0.02);
            }
        });

        sleep(1);

        // Connect HTTP client socket
        Stdio.File client = Stdio.File();
        runner->assert_true(client->connect("127.0.0.1", port));

        // 1. GET /api/v1/status
        string req = "GET /api/v1/status HTTP/1.1\r\nHost: localhost:18080\r\n\r\n";
        client->write(req);
        sleep(1);
        string resp = client->read(1024, 1);

        runner->assert_true(has_prefix(resp, "HTTP/1.1 200 OK"));
        runner->assert_true(search(resp, "\"status\":\"ok\"") >= 0 || search(resp, "status") >= 0);
        runner->assert_true(search(resp, "version") >= 0);

        client->close();

        // 2. GET /api/v1/metrics
        Stdio.File client2 = Stdio.File();
        runner->assert_true(client2->connect("127.0.0.1", port));
        string req2 = "GET /api/v1/metrics HTTP/1.1\r\nHost: localhost:18080\r\n\r\n";
        client2->write(req2);
        sleep(1);
        string resp2 = client2->read(4096, 1);

        runner->assert_true(has_prefix(resp2, "HTTP/1.1 200 OK"));
        runner->assert_true(search(resp2, "binnacle_uptime_seconds") >= 0);
        runner->assert_true(search(resp2, "binnacle_connected_clients") >= 0);

        client2->close();
        run = 0;
        http->stop();
        broker->stop();
    });

    return runner->report();
}
