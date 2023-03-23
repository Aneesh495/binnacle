// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/unit/test_packet_codec.pike
// Description: Unit tests for packet serialization and deserialization via PacketParser.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("MQTT Packet Serialization and Deserialization");

    runner->test("CONNECT packet round-trip", lambda() {
        object pkt = Binnacle.Protocol.ConnectPacket("client-xyz-01", Binnacle.Constants.PROTOCOL_LEVEL_5_0, 1, 60);
        pkt->set_auth("test-user", "secret-pass");
        string wire = pkt->encode();
        runner->assert_true(sizeof(wire) > 0, "Serialized data should be non-empty");

        array res = Binnacle.Protocol.PacketParser.parse_packet(wire);
        runner->assert_not_null(res, "Parser should successfully parse complete packet");
        object parsed = res[0];
        runner->assert_equal(parsed->packet_type, Binnacle.Constants.PACKET_CONNECT);
        runner->assert_equal(parsed->client_id, "client-xyz-01");
        runner->assert_equal(parsed->keep_alive, 60);
        runner->assert_equal(parsed->clean_start, 1);
        runner->assert_equal(parsed->username, "test-user");
        runner->assert_equal(parsed->password, "secret-pass");
    });

    runner->test("CONNACK packet round-trip", lambda() {
        mapping props = ([
            Binnacle.Constants.PROP_RECEIVE_MAXIMUM: 256
        ]);
        object pkt = Binnacle.Protocol.ConnAckPacket(0, Binnacle.Constants.RC_SUCCESS, props);
        string wire = pkt->encode();

        array res = Binnacle.Protocol.PacketParser.parse_packet(wire);
        runner->assert_not_null(res);
        object parsed = res[0];
        runner->assert_equal(parsed->packet_type, Binnacle.Constants.PACKET_CONNACK);
        runner->assert_equal(parsed->session_present, 0);
        runner->assert_equal(parsed->reason_code, Binnacle.Constants.RC_SUCCESS);
        runner->assert_equal(parsed->properties[Binnacle.Constants.PROP_RECEIVE_MAXIMUM], 256);
    });

    runner->test("PUBLISH QoS 0 round-trip", lambda() {
        object pkt = Binnacle.Protocol.PublishPacket("sensors/temperature", "22.5 C", 0, 0, 0, 0);
        string wire = pkt->encode();

        array res = Binnacle.Protocol.PacketParser.parse_packet(wire);
        runner->assert_not_null(res);
        object parsed = res[0];
        runner->assert_equal(parsed->packet_type, Binnacle.Constants.PACKET_PUBLISH);
        runner->assert_equal(parsed->topic, "sensors/temperature");
        runner->assert_equal(parsed->payload, "22.5 C");
        runner->assert_equal(parsed->qos, 0);
        runner->assert_equal(parsed->retain, 0);
    });

    runner->test("PUBLISH QoS 1 round-trip with packet ID", lambda() {
        object pkt = Binnacle.Protocol.PublishPacket("telemetry/speed", "105.4", 1, 0, 0, 42);
        string wire = pkt->encode();

        array res = Binnacle.Protocol.PacketParser.parse_packet(wire);
        runner->assert_not_null(res);
        object parsed = res[0];
        runner->assert_equal(parsed->packet_type, Binnacle.Constants.PACKET_PUBLISH);
        runner->assert_equal(parsed->topic, "telemetry/speed");
        runner->assert_equal(parsed->payload, "105.4");
        runner->assert_equal(parsed->qos, 1);
        runner->assert_equal(parsed->packet_id, 42);
    });

    runner->test("PUBACK packet round-trip", lambda() {
        object pkt = Binnacle.Protocol.PubAckPacket(42, Binnacle.Constants.RC_SUCCESS);
        string wire = pkt->encode();

        array res = Binnacle.Protocol.PacketParser.parse_packet(wire);
        runner->assert_not_null(res);
        object parsed = res[0];
        runner->assert_equal(parsed->packet_type, Binnacle.Constants.PACKET_PUBACK);
        runner->assert_equal(parsed->packet_id, 42);
        runner->assert_equal(parsed->reason_code, Binnacle.Constants.RC_SUCCESS);
    });

    runner->test("SUBSCRIBE and SUBACK packets round-trip", lambda() {
        object sub = Binnacle.Protocol.SubscribePacket(101);
        sub->add_subscription("sensors/+/temperature", 1);
        sub->add_subscription("fleet/#", 2);
        string wire = sub->encode();

        array res = Binnacle.Protocol.PacketParser.parse_packet(wire);
        runner->assert_not_null(res);
        object parsed = res[0];
        runner->assert_equal(parsed->packet_type, Binnacle.Constants.PACKET_SUBSCRIBE);
        runner->assert_equal(parsed->packet_id, 101);
        runner->assert_equal(sizeof(parsed->subscriptions), 2);
        runner->assert_equal(parsed->subscriptions[0][0], "sensors/+/temperature");
        runner->assert_equal(parsed->subscriptions[1][0], "fleet/#");

        // Test SUBACK
        object suback = Binnacle.Protocol.SubAckPacket(101, ({ 1, 2 }));
        string suback_wire = suback->encode();
        array suback_res = Binnacle.Protocol.PacketParser.parse_packet(suback_wire);
        runner->assert_not_null(suback_res);
        object parsed_suback = suback_res[0];
        runner->assert_equal(parsed_suback->packet_type, Binnacle.Constants.PACKET_SUBACK);
        runner->assert_equal(parsed_suback->packet_id, 101);
        runner->assert_equal(parsed_suback->reason_codes[0], 1);
        runner->assert_equal(parsed_suback->reason_codes[1], 2);
    });

    runner->test("PINGREQ and PINGRESP packets", lambda() {
        object pingreq = Binnacle.Protocol.PingReqPacket();
        string req_wire = pingreq->encode();
        array res_req = Binnacle.Protocol.PacketParser.parse_packet(req_wire);
        runner->assert_not_null(res_req);
        runner->assert_equal(res_req[0]->packet_type, Binnacle.Constants.PACKET_PINGREQ);

        object pingresp = Binnacle.Protocol.PingRespPacket();
        string resp_wire = pingresp->encode();
        array res_resp = Binnacle.Protocol.PacketParser.parse_packet(resp_wire);
        runner->assert_not_null(res_resp);
        runner->assert_equal(res_resp[0]->packet_type, Binnacle.Constants.PACKET_PINGRESP);
    });

    runner->test("DISCONNECT packet round-trip", lambda() {
        object pkt = Binnacle.Protocol.DisconnectPacket(Binnacle.Constants.RC_NORMAL_DISCONNECTION);
        string wire = pkt->encode();

        array res = Binnacle.Protocol.PacketParser.parse_packet(wire);
        runner->assert_not_null(res);
        object parsed = res[0];
        runner->assert_equal(parsed->packet_type, Binnacle.Constants.PACKET_DISCONNECT);
        runner->assert_equal(parsed->reason_code, Binnacle.Constants.RC_NORMAL_DISCONNECTION);
    });

    return runner->report();
}
