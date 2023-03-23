// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/unit/test_varint.pike
// Description: Unit tests for OASIS MQTT 5.0 Variable Byte Integer codec.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("MQTT 5.0 Variable Byte Integer Codec");

    runner->test("Encode and decode boundary zero (0)", lambda() {
        string encoded = Binnacle.Protocol.VarInt.encode(0);
        runner->assert_equal(sizeof(encoded), 1, "Length of 0 should be 1 byte");
        runner->assert_equal(encoded[0], 0x00, "Encoded byte should be 0x00");

        array res = Binnacle.Protocol.VarInt.decode(encoded, 0);
        runner->assert_equal(res[0], 0, "Decoded value should be 0");
        runner->assert_equal(res[1], 1, "Consumed bytes should be 1");
    });

    runner->test("Encode and decode single-byte limit (127)", lambda() {
        string encoded = Binnacle.Protocol.VarInt.encode(127);
        runner->assert_equal(sizeof(encoded), 1, "Length of 127 should be 1 byte");
        runner->assert_equal(encoded[0], 0x7F, "Encoded byte should be 0x7F");

        array res = Binnacle.Protocol.VarInt.decode(encoded, 0);
        runner->assert_equal(res[0], 127, "Decoded value should be 127");
        runner->assert_equal(res[1], 1, "Consumed bytes should be 1");
    });

    runner->test("Encode and decode two-byte boundary (128)", lambda() {
        string encoded = Binnacle.Protocol.VarInt.encode(128);
        runner->assert_equal(sizeof(encoded), 2, "Length of 128 should be 2 bytes");
        runner->assert_equal(encoded[0], 0x80, "First byte should have continuation bit");
        runner->assert_equal(encoded[1], 0x01, "Second byte should be 0x01");

        array res = Binnacle.Protocol.VarInt.decode(encoded, 0);
        runner->assert_equal(res[0], 128, "Decoded value should be 128");
        runner->assert_equal(res[1], 2, "Consumed bytes should be 2");
    });

    runner->test("Encode and decode two-byte limit (16383)", lambda() {
        string encoded = Binnacle.Protocol.VarInt.encode(16383);
        runner->assert_equal(sizeof(encoded), 2, "Length should be 2 bytes");

        array res = Binnacle.Protocol.VarInt.decode(encoded, 0);
        runner->assert_equal(res[0], 16383, "Decoded value should be 16383");
        runner->assert_equal(res[1], 2, "Consumed bytes should be 2");
    });

    runner->test("Encode and decode three-byte boundary (16384)", lambda() {
        string encoded = Binnacle.Protocol.VarInt.encode(16384);
        runner->assert_equal(sizeof(encoded), 3, "Length should be 3 bytes");

        array res = Binnacle.Protocol.VarInt.decode(encoded, 0);
        runner->assert_equal(res[0], 16384, "Decoded value should be 16384");
        runner->assert_equal(res[1], 3, "Consumed bytes should be 3");
    });

    runner->test("Encode and decode maximum 4-byte limit (268435455)", lambda() {
        string encoded = Binnacle.Protocol.VarInt.encode(268435455);
        runner->assert_equal(sizeof(encoded), 4, "Length should be 4 bytes");

        array res = Binnacle.Protocol.VarInt.decode(encoded, 0);
        runner->assert_equal(res[0], 268435455, "Decoded value should match max");
        runner->assert_equal(res[1], 4, "Consumed bytes should be 4");
    });

    runner->test("Reject negative and out of bound numbers", lambda() {
        runner->assert_throws(lambda() {
            Binnacle.Protocol.VarInt.encode(-1);
        }, "Negative numbers must throw");

        runner->assert_throws(lambda() {
            Binnacle.Protocol.VarInt.encode(268435456);
        }, "Values > 268435455 must throw");
    });

    runner->test("Incomplete buffer throws MalformedPacketError", lambda() {
        string partial = sprintf("%c", 0x80); // Continuation bit set, but only 1 byte
        runner->assert_throws(lambda() {
            Binnacle.Protocol.VarInt.decode(partial, 0);
        }, "Partial buffer must throw MalformedPacketError");
    });

    return runner->report();
}
