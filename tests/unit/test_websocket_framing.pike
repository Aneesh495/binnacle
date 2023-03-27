// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/unit/test_websocket_framing.pike
// Description: Unit tests for RFC 6455 WebSocket framing, masking, and HTTP handshake.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("RFC 6455 WebSocket Framing and Codec");

    runner->test("Unmasked binary frame round-trip (< 126 bytes)", lambda() {
        object frame = Binnacle.Transport.WebSocketFrame(
            Binnacle.Transport.WebSocketFrame.OPCODE_BINARY, "hello-mqtt-broker"
        );
        string wire = frame->encode();

        array res = Binnacle.Transport.WebSocketParser.decode_frame(wire);
        runner->assert_not_null(res);
        object parsed = res[0];
        runner->assert_equal(parsed->opcode, Binnacle.Transport.WebSocketFrame.OPCODE_BINARY);
        runner->assert_equal(parsed->payload, "hello-mqtt-broker");
        runner->assert_equal(parsed->fin, 1);
    });

    runner->test("Medium payload frame (126-65535 bytes with 16-bit length)", lambda() {
        string medium_payload = "A" * 500;
        object frame = Binnacle.Transport.WebSocketFrame(
            Binnacle.Transport.WebSocketFrame.OPCODE_BINARY, medium_payload
        );
        string wire = frame->encode();

        array res = Binnacle.Transport.WebSocketParser.decode_frame(wire);
        runner->assert_not_null(res);
        object parsed = res[0];
        runner->assert_equal(sizeof(parsed->payload), 500);
        runner->assert_equal(parsed->payload, medium_payload);
    });

    runner->test("Client masked frame unmasking", lambda() {
        // Construct a masked frame: fin=1, opcode=binary, masked=1, length=4
        string mask_key = sprintf("%c%c%c%c", 0x12, 0x34, 0x56, 0x78);
        string plain = "TEST";
        string masked = "";
        for (int i = 0; i < sizeof(plain); i++) {
            masked += sprintf("%c", plain[i] ^ mask_key[i % 4]);
        }

        string wire = sprintf("%c%c%s%s", 0x82, 0x84, mask_key, masked);
        array res = Binnacle.Transport.WebSocketParser.decode_frame(wire);
        runner->assert_not_null(res);
        object parsed = res[0];
        runner->assert_equal(parsed->payload, "TEST");
    });

    runner->test("WebSocket Upgrade HTTP 101 Handshake generation", lambda() {
        string req = "GET /mqtt HTTP/1.1\r\n" +
                     "Host: localhost:8080\r\n" +
                     "Upgrade: websocket\r\n" +
                     "Connection: Upgrade\r\n" +
                     "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
                     "Sec-WebSocket-Protocol: mqtt\r\n\r\n";

        string resp = Binnacle.Transport.WebSocketParser.generate_handshake_response(req);
        runner->assert_not_null(resp);
        runner->assert_true(has_prefix(resp, "HTTP/1.1 101 Switching Protocols"));
        // RFC 6455 known test key outcome
        runner->assert_true(search(resp, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") >= 0);
    });

    return runner->report();
}
