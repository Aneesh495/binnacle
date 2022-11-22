// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Base Packet Class

int packet_type;
int flags;
int qos;
int dup;
int retain;
int remaining_length;
mapping(int:mixed) properties;

void create(int _type, int|void _flags, mapping(int:mixed)|void _props) {
    packet_type = _type;
    flags = zero_type(_flags) ? 0 : _flags;
    properties = _props || ([]);
    qos = 0;
    dup = 0;
    retain = 0;
    remaining_length = 0;
}

//! Encode fixed header prefix with packet type and variable-byte remaining length.
string encode_fixed_header(int rem_len, int|void override_flags) {
    int f = zero_type(override_flags) ? flags : override_flags;
    int first_byte = ((packet_type & 0x0F) << 4) | (f & 0x0F);
    string len_bytes = Binnacle.Protocol.VarInt.encode(rem_len);
    return sprintf("%c%s", first_byte, len_bytes);
}

//! Virtual method for encoding packet to binary wire format.
string encode(int|void protocol_version) {
    error("Virtual method encode() called on base Packet\n");
}

string type_name() {
    return Binnacle.Constants.packet_type_name(packet_type);
}
