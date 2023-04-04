// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/regressions/test_bug_websocket_masking.pike
// Description: Regression test ensuring client-to-server RFC 6455 XOR masking handles extended 16-bit payload lengths.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Regression: WebSocket 16-Bit Length XOR Mask Offset");

    runner->test("Bug Fix: 16-bit extended payload length shifts 4-byte mask key to byte offset 4", lambda() {
        // Construct a 200-byte payload (> 125 bytes, so length code is 126 followed by 2-byte length)
        string plain = "A" * 200;
        string mask_key = sprintf("%c%c%c%c", 0xAA, 0xBB, 0xCC, 0xDD);
        string masked = "";
        for (int i = 0; i < sizeof(plain); i++) {
            masked += sprintf("%c", plain[i] ^ mask_key[i % 4]);
        }

        // Fixed header:
        // Byte 0: 0x82 (fin=1, opcode=2 binary)
        // Byte 1: 0xFE (mask=1, length=126 indicating 16-bit extended length follows)
        // Bytes 2-3: 200 (16-bit big-endian length)
        // Bytes 4-7: mask_key
        // Bytes 8+: masked payload
        string wire = sprintf("%c%c%2c%s%s", 0x82, 0xFE, 200, mask_key, masked);

        array res = Binnacle.Transport.WebSocketParser.decode_frame(wire);
        runner->assert_not_null(res, "Frame must be decoded successfully");
        object parsed = res[0];
        runner->assert_equal(sizeof(parsed->payload), 200);
        runner->assert_equal(parsed->payload, plain);
    });

    return runner->report();
}
