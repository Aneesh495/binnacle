// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// PINGRESP Packet Model and Codec (OASIS MQTT 5.0 Section 3.13)

inherit .Packet;

void create() {
    ::create(Binnacle.Constants.PACKET_PINGRESP, 0x00);
}

string encode(int|void protocol_version) {
    return encode_fixed_header(0, 0x00);
}
