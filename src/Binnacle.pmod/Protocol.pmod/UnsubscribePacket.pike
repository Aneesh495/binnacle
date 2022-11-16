// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// UNSUBSCRIBE Packet Model and Codec (OASIS MQTT 5.0 Section 3.10)

class UnsubscribePacket {
    inherit Binnacle.Protocol.Packet;

    int packet_id;
    array(string) topic_filters;

    void create(int _pid, array(string)|void _filters, mapping(int:mixed)|void _props) {
        // Fixed header flags MUST be 0x02 for UNSUBSCRIBE
        ::create(Binnacle.Constants.PACKET_UNSUBSCRIBE, 0x02, _props);
        packet_id = _pid;
        topic_filters = _filters || ({});
    }

    void add_topic_filter(string filter) {
        topic_filters += ({ filter });
    }

    string encode(int|void protocol_version) {
        int ver = zero_type(protocol_version) ? Binnacle.Constants.PROTOCOL_LEVEL_5_0 : protocol_version;
        string var_hdr = sprintf("%2c", packet_id & 0xFFFF);

        if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
            var_hdr += Binnacle.Protocol.Properties.encode_properties(properties);
        }

        string payload = "";
        foreach (topic_filters, string filter) {
            payload += sprintf("%2c%s", sizeof(filter), filter);
        }

        int rem_len = sizeof(var_hdr) + sizeof(payload);
        return encode_fixed_header(rem_len, 0x02) + var_hdr + payload;
    }
}
