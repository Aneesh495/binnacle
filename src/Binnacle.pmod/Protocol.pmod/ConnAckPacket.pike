// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// CONNACK Packet Model and Codec (OASIS MQTT 5.0 Section 3.2)

class ConnAckPacket {
    inherit Binnacle.Protocol.Packet;

    int session_present;
    int reason_code;

    void create(int|void _sp, int|void _rc, mapping(int:mixed)|void _props) {
        ::create(Binnacle.Constants.PACKET_CONNACK, 0x00, _props);
        session_present = zero_type(_sp) ? 0 : _sp;
        reason_code = zero_type(_rc) ? Binnacle.Constants.RC_SUCCESS : _rc;
    }

    string encode(int|void protocol_version) {
        int ver = zero_type(protocol_version) ? Binnacle.Constants.PROTOCOL_LEVEL_5_0 : protocol_version;
        int flags_byte = session_present ? 0x01 : 0x00;

        string var_hdr = sprintf("%c%c", flags_byte, reason_code);

        if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
            var_hdr += Binnacle.Protocol.Properties.encode_properties(properties);
        }

        int rem_len = sizeof(var_hdr);
        return encode_fixed_header(rem_len, 0x00) + var_hdr;
    }
}
