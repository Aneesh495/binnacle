// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// AUTH Packet Model and Codec (OASIS MQTT 5.0 Section 3.15)

inherit .Packet;

int reason_code;

void create(int|void _rc, mapping(int:mixed)|void _props) {
    ::create(Binnacle.Constants.PACKET_AUTH, 0x00, _props);
    reason_code = zero_type(_rc) ? Binnacle.Constants.RC_SUCCESS : _rc;
}

string encode(int|void protocol_version) {
    int ver = zero_type(protocol_version) ? Binnacle.Constants.PROTOCOL_LEVEL_5_0 : protocol_version;
    if (ver < Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
        throw(Binnacle.Errors.ProtocolError(
            "AUTH packet is not supported in MQTT versions prior to 5.0",
            Binnacle.Constants.RC_PROTOCOL_ERROR
        ));
    }

    string var_hdr = sprintf("%c", reason_code);
    var_hdr += Binnacle.Protocol.Properties.encode_properties(properties);

    int rem_len = sizeof(var_hdr);
    return encode_fixed_header(rem_len, 0x00) + var_hdr;
}
