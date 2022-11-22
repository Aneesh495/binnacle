// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// MQTT Fixed Header Model and Validator

int packet_type;
int flags;
int dup;
int qos;
int retain;
int remaining_length;
int header_length; // Number of bytes in fixed header (1 byte type/flags + varint)

void create(int _type, int _flags, int _rem_len, int _hdr_len) {
    packet_type = _type;
    flags = _flags;
    remaining_length = _rem_len;
    header_length = _hdr_len;

    if (packet_type == Binnacle.Constants.PACKET_PUBLISH) {
        dup = (flags & 0x08) ? 1 : 0;
        qos = (flags >> 1) & 0x03;
        retain = (flags & 0x01) ? 1 : 0;
    } else {
        dup = 0;
        qos = 0;
        retain = 0;
    }
}

//! Validate fixed header flags according to MQTT 5.0 Table 2-2.
int validate_flags() {
    switch (packet_type) {
        case Binnacle.Constants.PACKET_CONNECT:
        case Binnacle.Constants.PACKET_CONNACK:
        case Binnacle.Constants.PACKET_PUBACK:
        case Binnacle.Constants.PACKET_PUBREC:
        case Binnacle.Constants.PACKET_PUBCOMP:
        case Binnacle.Constants.PACKET_SUBACK:
        case Binnacle.Constants.PACKET_UNSUBACK:
        case Binnacle.Constants.PACKET_PINGREQ:
        case Binnacle.Constants.PACKET_PINGRESP:
        case Binnacle.Constants.PACKET_DISCONNECT:
        case Binnacle.Constants.PACKET_AUTH:
            return flags == 0x00;

        case Binnacle.Constants.PACKET_PUBREL:
        case Binnacle.Constants.PACKET_SUBSCRIBE:
        case Binnacle.Constants.PACKET_UNSUBSCRIBE:
            return flags == 0x02;

        case Binnacle.Constants.PACKET_PUBLISH:
            if (qos == 3) return 0;
            if (qos == 0 && dup != 0) return 0;
            return 1;

        default:
            return 0;
    }
}
