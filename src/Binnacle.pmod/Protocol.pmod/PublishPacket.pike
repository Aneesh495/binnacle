// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// PUBLISH Packet Model and Codec (OASIS MQTT 5.0 Section 3.3)

inherit .Packet;

string topic;
int packet_id;
string payload;

void create(string _topic, string _payload, int|void _qos, int|void _retain,
            int|void _dup, int|void _pid, mapping(int:mixed)|void _props) {
    int initial_qos = zero_type(_qos) ? 0 : _qos;
    int initial_retain = zero_type(_retain) ? 0 : _retain;
    int initial_dup = zero_type(_dup) ? 0 : _dup;
    int f = (initial_dup ? 0x08 : 0x00) | ((initial_qos & 0x03) << 1) | (initial_retain ? 0x01 : 0x00);

    ::create(Binnacle.Constants.PACKET_PUBLISH, f, _props);
    topic = _topic || "";
    payload = _payload || "";
    qos = initial_qos;
    retain = initial_retain;
    dup = initial_dup;
    packet_id = zero_type(_pid) ? 0 : _pid;
}

string encode(int|void protocol_version) {
    int ver = zero_type(protocol_version) ? Binnacle.Constants.PROTOCOL_LEVEL_5_0 : protocol_version;
    string var_hdr = sprintf("%2c%s", sizeof(topic), topic);

    if (qos > 0) {
        var_hdr += sprintf("%2c", packet_id & 0xFFFF);
    }

    if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
        var_hdr += Binnacle.Protocol.Properties.encode_properties(properties);
    }

    int rem_len = sizeof(var_hdr) + sizeof(payload);
    int f = (dup ? 0x08 : 0x00) | ((qos & 0x03) << 1) | (retain ? 0x01 : 0x00);
    return encode_fixed_header(rem_len, f) + var_hdr + payload;
}
