// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// SUBSCRIBE Packet Model and Codec (OASIS MQTT 5.0 Section 3.8)

class SubscribePacket {
    inherit Binnacle.Protocol.Packet;

    int packet_id;
    // subscriptions is an array of ({ string topic_filter, int options_byte })
    array(array) subscriptions;

    void create(int _pid, array(array)|void _subs, mapping(int:mixed)|void _props) {
        // Fixed header flags MUST be 0x02 for SUBSCRIBE
        ::create(Binnacle.Constants.PACKET_SUBSCRIBE, 0x02, _props);
        packet_id = _pid;
        subscriptions = _subs || ({});
    }

    void add_subscription(string filter, int|void qos, int|void nl, int|void rap, int|void rh) {
        int opt_qos = zero_type(qos) ? 0 : (qos & 0x03);
        int opt_nl  = zero_type(nl) ? 0 : (nl ? 0x04 : 0x00);
        int opt_rap = zero_type(rap) ? 0 : (rap ? 0x08 : 0x00);
        int opt_rh  = zero_type(rh) ? 0 : ((rh & 0x03) << 4);
        int options_byte = opt_qos | opt_nl | opt_rap | opt_rh;
        subscriptions += ({ ({ filter, options_byte }) });
    }

    string encode(int|void protocol_version) {
        int ver = zero_type(protocol_version) ? Binnacle.Constants.PROTOCOL_LEVEL_5_0 : protocol_version;
        string var_hdr = sprintf("%2c", packet_id & 0xFFFF);

        if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
            var_hdr += Binnacle.Protocol.Properties.encode_properties(properties);
        }

        string payload = "";
        foreach (subscriptions, array sub) {
            string filter = sub[0];
            int opt = sub[1];
            payload += sprintf("%2c%s%c", sizeof(filter), filter, opt);
        }

        int rem_len = sizeof(var_hdr) + sizeof(payload);
        return encode_fixed_header(rem_len, 0x02) + var_hdr + payload;
    }
}
