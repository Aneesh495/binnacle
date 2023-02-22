// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// RFC 6455 WebSocket Frame Model and Codec

constant OPCODE_CONTINUATION = 0x00;
constant OPCODE_TEXT         = 0x01;
constant OPCODE_BINARY       = 0x02;
constant OPCODE_CLOSE        = 0x08;
constant OPCODE_PING         = 0x09;
constant OPCODE_PONG         = 0x0A;

int fin;
int opcode;
int masked;
string payload;

void create(int _op, string _data, int|void _fin) {
    opcode = _op;
    payload = _data || "";
    fin = zero_type(_fin) ? 1 : _fin;
    masked = 0;
}

//! Encode frame to binary string for transmission to browser client.
//! Server-to-client frames MUST NOT be masked per RFC 6455.
string encode() {
    int b1 = (fin ? 0x80 : 0x00) | (opcode & 0x0F);
    int p_len = sizeof(payload);
    string header = sprintf("%c", b1);

    if (p_len <= 125) {
        header += sprintf("%c", p_len);
    } else if (p_len <= 65535) {
        header += sprintf("%c%2c", 126, p_len);
    } else {
        header += sprintf("%c%8c", 127, p_len);
    }

    return header + payload;
}
