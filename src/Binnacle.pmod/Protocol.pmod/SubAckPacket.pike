// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// SUBACK Packet Model and Codec (OASIS MQTT 5.0 Section 3.9)

inherit .Packet;

int packet_id;
array(int) reason_codes;

void create(int _pid, array(int)|void _rcs, mapping(int:mixed)|void _props) {
    ::create(Binnacle.Constants.PACKET_SUBACK, 0x00, _props);
    packet_id = _pid;
    reason_codes = _rcs || ({});
}

void add_reason_code(int rc) {
    reason_codes += ({ rc & 0xFF });
}

string encode(int|void protocol_version) {
    int ver = zero_type(protocol_version) ? Binnacle.Constants.PROTOCOL_LEVEL_5_0 : protocol_version;
    string var_hdr = sprintf("%2c", packet_id & 0xFFFF);

    if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
        var_hdr += Binnacle.Protocol.Properties.encode_properties(properties);
    }

    string payload = "";
    foreach (reason_codes, int rc) {
        payload += sprintf("%c", rc & 0xFF);
    }

    int rem_len = sizeof(var_hdr) + sizeof(payload);
    return encode_fixed_header(rem_len, 0x00) + var_hdr + payload;
}
