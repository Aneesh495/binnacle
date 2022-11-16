// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// CONNECT Packet Model and Codec (OASIS MQTT 5.0 Section 3.1)

class ConnectPacket {
    inherit Binnacle.Protocol.Packet;

    string protocol_name;
    int protocol_version;
    int clean_start;
    int will_flag;
    int will_qos;
    int will_retain;
    int password_flag;
    int username_flag;
    int keep_alive;
    string client_id;
    string will_topic;
    string will_payload;
    mapping(int:mixed) will_properties;
    string username;
    string password;

    void create(string|void _client_id, int|void _ver, int|void _clean, int|void _ka) {
        ::create(Binnacle.Constants.PACKET_CONNECT, 0x00);
        protocol_name = Binnacle.Constants.PROTOCOL_NAME_MQTT;
        protocol_version = zero_type(_ver) ? Binnacle.Constants.PROTOCOL_LEVEL_5_0 : _ver;
        clean_start = zero_type(_clean) ? 1 : _clean;
        keep_alive = zero_type(_ka) ? 60 : _ka;
        client_id = _client_id || "";
        will_flag = 0;
        will_qos = 0;
        will_retain = 0;
        password_flag = 0;
        username_flag = 0;
        will_properties = ([]);
    }

    void set_will(string topic, string payload, int qos, int retain,
                  mapping(int:mixed)|void props) {
        will_flag = 1;
        will_topic = topic;
        will_payload = payload;
        will_qos = qos;
        will_retain = retain;
        will_properties = props || ([]);
    }

    void set_auth(string user, string pass) {
        if (user && sizeof(user)) {
            username_flag = 1;
            username = user;
        }
        if (pass && sizeof(pass)) {
            password_flag = 1;
            password = pass;
        }
    }

    string encode(int|void override_ver) {
        int ver = zero_type(override_ver) ? protocol_version : override_ver;
        string var_hdr = sprintf("%2c%s%c", sizeof(protocol_name), protocol_name, ver);

        int connect_flags = 0;
        if (clean_start)   connect_flags |= Binnacle.Constants.CONNECT_FLAG_CLEAN_START;
        if (will_flag) {
            connect_flags |= Binnacle.Constants.CONNECT_FLAG_WILL;
            connect_flags |= (will_qos & 0x03) << 3;
            if (will_retain) connect_flags |= Binnacle.Constants.CONNECT_FLAG_WILL_RETAIN;
        }
        if (password_flag) connect_flags |= Binnacle.Constants.CONNECT_FLAG_PASSWORD;
        if (username_flag) connect_flags |= Binnacle.Constants.CONNECT_FLAG_USERNAME;

        var_hdr += sprintf("%c%2c", connect_flags, keep_alive);

        if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
            var_hdr += Binnacle.Protocol.Properties.encode_properties(properties);
        }

        string payload_bytes = sprintf("%2c%s", sizeof(client_id), client_id);

        if (will_flag) {
            if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
                payload_bytes += Binnacle.Protocol.Properties.encode_properties(will_properties);
            }
            payload_bytes += sprintf("%2c%s", sizeof(will_topic), will_topic);
            payload_bytes += sprintf("%2c%s", sizeof(will_payload), will_payload);
        }

        if (username_flag && username) {
            payload_bytes += sprintf("%2c%s", sizeof(username), username);
        }

        if (password_flag && password) {
            payload_bytes += sprintf("%2c%s", sizeof(password), password);
        }

        int rem_len = sizeof(var_hdr) + sizeof(payload_bytes);
        return encode_fixed_header(rem_len, 0x00) + var_hdr + payload_bytes;
    }
}
