// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// PUBREL Packet Model and Codec (OASIS MQTT 5.0 Section 3.6)

inherit .Packet;

int packet_id;
int reason_code;

void create(int _pid, int|void _rc, mapping(int:mixed)|void _props) {
    // Fixed header flags MUST be 0x02 for PUBREL
    ::create(Binnacle.Constants.PACKET_PUBREL, 0x02, _props);
    packet_id = _pid;
    reason_code = zero_type(_rc) ? Binnacle.Constants.RC_SUCCESS : _rc;
}

string encode(int|void protocol_version) {
    int ver = zero_type(protocol_version) ? Binnacle.Constants.PROTOCOL_LEVEL_5_0 : protocol_version;
    string var_hdr = sprintf("%2c", packet_id & 0xFFFF);

    if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
        if (reason_code != Binnacle.Constants.RC_SUCCESS || (properties && sizeof(properties))) {
            var_hdr += sprintf("%c", reason_code);
            var_hdr += Binnacle.Protocol.Properties.encode_properties(properties);
        }
    }

    int rem_len = sizeof(var_hdr);
    return encode_fixed_header(rem_len, 0x02) + var_hdr;
}
