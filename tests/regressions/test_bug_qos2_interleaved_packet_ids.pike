// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/regressions/test_bug_qos2_interleaved_packet_ids.pike
// Description: Regression test ensuring interleaved concurrent QoS 2 packet identifiers maintain strict isolation.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Regression: Interleaved QoS 2 Packet ID State Isolation");

    runner->test("Bug Fix: Interleaved QoS 2 packets do not corrupt inflight state", lambda() {
        object win = Binnacle.Engine.InflightWindow(10);

        object m1 = Binnacle.Protocol.PublishPacket("telemetry/a", "p1", 2, 0, 0, 101);
        object m2 = Binnacle.Protocol.PublishPacket("telemetry/b", "p2", 2, 0, 0, 102);
        object m3 = Binnacle.Protocol.PublishPacket("telemetry/c", "p3", 2, 0, 0, 103);

        win->add(m1);
        win->add(m2);
        win->add(m3);

        runner->assert_equal(win->size(), 3);

        // Acknowledge middle message (102) out of order
        object acked102 = win->acknowledge(102);
        runner->assert_not_null(acked102);
        runner->assert_equal(acked102->packet_id, 102);
        runner->assert_equal(win->size(), 2);

        // Acknowledge 101
        object acked101 = win->acknowledge(101);
        runner->assert_not_null(acked101);
        runner->assert_equal(acked101->packet_id, 101);

        // Acknowledge 103
        object acked103 = win->acknowledge(103);
        runner->assert_not_null(acked103);
        runner->assert_equal(acked103->packet_id, 103);
        runner->assert_equal(win->size(), 0);
    });

    return runner->report();
}
