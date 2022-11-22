// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// PINGREQ Packet Model and Codec (OASIS MQTT 5.0 Section 3.12)

inherit .Packet;

void create() {
    ::create(Binnacle.Constants.PACKET_PINGREQ, 0x00);
}

string encode(int|void protocol_version) {
    return encode_fixed_header(0, 0x00);
}
