// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/regressions/test_bug_varint_overflow.pike
// Description: Regression test ensuring variable byte integers cannot overflow 28-bit maximum.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Regression: VarInt Overflow and Continuation Limit Guard");

    runner->test("Bug Fix: Values greater than 268,435,455 throw MalformedPacketError", lambda() {
        runner->assert_throws(lambda() {
            Binnacle.Protocol.VarInt.encode(268435456);
        }, "Values above 268435455 must throw MalformedPacketError");

        runner->assert_throws(lambda() {
            Binnacle.Protocol.VarInt.encode(1000000000);
        }, "Large integers must throw MalformedPacketError");
    });

    runner->test("Bug Fix: 5-byte continuation sequences are rejected as malformed", lambda() {
        // Wire stream with 5 consecutive bytes having continuation bit 0x80 set
        string bad_wire = sprintf("%c%c%c%c%c", 0x80, 0x80, 0x80, 0x80, 0x80);

        runner->assert_throws(lambda() {
            Binnacle.Protocol.VarInt.decode(bad_wire, 0);
        }, "Continuation exceeding 4 bytes must be rejected");
    });

    return runner->report();
}
