// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Inter-Broker Cluster Replication Protocol Frame Codec

constant MESH_MAGIC = "BMESH";
constant MSG_HELLO              = 1;
constant MSG_HEARTBEAT          = 2;
constant MSG_FORWARD_PUBLISH    = 3;
constant MSG_TOPIC_INTEREST     = 4;

//! Encode an inter-broker cluster message frame.
string encode_frame(int msg_type, string origin_node, int hops, string payload) {
    int o_len = sizeof(origin_node);
    int p_len = sizeof(payload);

    string header = sprintf("%s%c%2c%s%c%4c",
                            MESH_MAGIC, msg_type, o_len, origin_node, hops & 0xFF, p_len);
    string full_frame = header + payload;
    string crc = Binnacle.Utils.Crc32.calculate_binary(full_frame);

    return full_frame + crc;
}

//! Decode an inter-broker cluster message frame from buffer.
//! Returns ({ mapping msg, int bytes_consumed }) or 0 if buffer incomplete.
array(mixed)|int decode_frame(string buf, int|void offset) {
    int idx = offset || 0;
    int len = sizeof(buf);

    if ((len - idx) < 17) return 0; // Minimum frame header + CRC

    if (buf[idx..idx+4] != MESH_MAGIC) {
        throw(Binnacle.Errors.ClusterError("Invalid cluster mesh frame magic"));
    }
    idx += 5;

    int msg_type = buf[idx++];
    int o_len;
    sscanf(buf[idx..idx+1], "%2c", o_len);
    idx += 2;

    if ((len - idx) < (o_len + 5 + 4)) return 0;

    string origin = buf[idx..idx+o_len-1];
    idx += o_len;

    int hops = buf[idx++];
    int p_len;
    sscanf(buf[idx..idx+3], "%4c", p_len);
    idx += 4;

    if ((len - idx) < (p_len + 4)) return 0;

    string payload = buf[idx..idx+p_len-1];
    idx += p_len;

    int expected_crc;
    sscanf(buf[idx..idx+3], "%4c", expected_crc);
    idx += 4;

    int start_pos = offset || 0;
    string checked = buf[start_pos .. idx - 5];
    int actual_crc = Binnacle.Utils.Crc32.calculate(checked);

    if (actual_crc != (expected_crc & 0xFFFFFFFF)) {
        throw(Binnacle.Errors.ClusterError("Cluster mesh frame CRC32 verification failed"));
    }

    mapping msg = ([
        "type": msg_type,
        "origin": origin,
        "hops": hops,
        "payload": payload
    ]);

    int total_consumed = idx - start_pos;
    return ({ msg, total_consumed });
}
