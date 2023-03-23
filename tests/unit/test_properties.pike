// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/unit/test_properties.pike
// Description: Unit tests for MQTT 5.0 Properties codec.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("MQTT 5.0 Property Codec");

    runner->test("Encode and decode session expiry interval", lambda() {
        mapping props = ([
            Binnacle.Constants.PROP_SESSION_EXPIRY_INTERVAL: 3600
        ]);
        string encoded = Binnacle.Protocol.Properties.encode_properties(props);
        runner->assert_true(sizeof(encoded) > 0, "Encoded string should not be empty");

        array res = Binnacle.Protocol.Properties.decode_properties(encoded, 0);
        mapping decoded = res[0];
        runner->assert_equal(decoded[Binnacle.Constants.PROP_SESSION_EXPIRY_INTERVAL], 3600, "Session expiry should match 3600");
    });

    runner->test("Encode and decode user property pairs", lambda() {
        mapping props = ([
            Binnacle.Constants.PROP_USER_PROPERTY: ({
                ({ "trace_id", "abc-123-xyz" }),
                ({ "origin", "edge-gateway-42" })
            })
        ]);
        string encoded = Binnacle.Protocol.Properties.encode_properties(props);
        array res = Binnacle.Protocol.Properties.decode_properties(encoded, 0);
        mapping decoded = res[0];

        runner->assert_true(arrayp(decoded[Binnacle.Constants.PROP_USER_PROPERTY]), "User properties must be an array");
        runner->assert_equal(sizeof(decoded[Binnacle.Constants.PROP_USER_PROPERTY]), 2, "Should have 2 pairs");
        runner->assert_equal(decoded[Binnacle.Constants.PROP_USER_PROPERTY][0][0], "trace_id");
        runner->assert_equal(decoded[Binnacle.Constants.PROP_USER_PROPERTY][0][1], "abc-123-xyz");
        runner->assert_equal(decoded[Binnacle.Constants.PROP_USER_PROPERTY][1][0], "origin");
        runner->assert_equal(decoded[Binnacle.Constants.PROP_USER_PROPERTY][1][1], "edge-gateway-42");
    });

    runner->test("Encode and decode payload format and content type", lambda() {
        mapping props = ([
            Binnacle.Constants.PROP_PAYLOAD_FORMAT_INDICATOR: 1,
            Binnacle.Constants.PROP_CONTENT_TYPE: "application/json"
        ]);
        string encoded = Binnacle.Protocol.Properties.encode_properties(props);
        array res = Binnacle.Protocol.Properties.decode_properties(encoded, 0);
        mapping decoded = res[0];

        runner->assert_equal(decoded[Binnacle.Constants.PROP_PAYLOAD_FORMAT_INDICATOR], 1);
        runner->assert_equal(decoded[Binnacle.Constants.PROP_CONTENT_TYPE], "application/json");
    });

    runner->test("Encode and decode correlation data and response topic", lambda() {
        mapping props = ([
            Binnacle.Constants.PROP_RESPONSE_TOPIC: "reply/rpc/worker",
            Binnacle.Constants.PROP_CORRELATION_DATA: "req_884920"
        ]);
        string encoded = Binnacle.Protocol.Properties.encode_properties(props);
        array res = Binnacle.Protocol.Properties.decode_properties(encoded, 0);
        mapping decoded = res[0];

        runner->assert_equal(decoded[Binnacle.Constants.PROP_RESPONSE_TOPIC], "reply/rpc/worker");
        runner->assert_equal(decoded[Binnacle.Constants.PROP_CORRELATION_DATA], "req_884920");
    });

    runner->test("Empty properties map encodes to single zero byte", lambda() {
        string encoded = Binnacle.Protocol.Properties.encode_properties(([]));
        runner->assert_equal(sizeof(encoded), 1, "Length of empty property block should be 1");
        runner->assert_equal(encoded[0], 0x00, "Zero byte indicates length 0");

        array res = Binnacle.Protocol.Properties.decode_properties(encoded, 0);
        runner->assert_equal(sizeof(indices(res[0])), 0, "Decoded map should be empty");
    });

    return runner->report();
}
