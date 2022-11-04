// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Variable Byte Integer Codec (MQTT 5.0 Section 1.5.5)

constant MAX_VARINT_VALUE = 268435455; // 0x0FFFFFFF (256 MB - 1)

//! Encode non-negative integer into variable-byte integer binary representation.
//! Throws MalformedPacketError if value exceeds 268,435,455 or is negative.
string encode(int val) {
    if (val < 0 || val > MAX_VARINT_VALUE) {
        throw(Binnacle.Errors.MalformedPacketError(
            sprintf("Variable byte integer out of range: %d", val),
            Binnacle.Constants.RC_MALFORMED_PACKET
        ));
    }

    string res = "";
    do {
        int byte_val = val & 0x7F;
        val = val >> 7;
        if (val > 0) {
            byte_val |= 0x80;
        }
        res += sprintf("%c", byte_val);
    } while (val > 0);

    return res;
}

//! Decode a variable-byte integer from buffer starting at offset.
//! Returns ({ value, bytes_consumed }).
//! Throws MalformedPacketError if buffer is truncated or continuation exceeds 4 bytes.
array(int) decode(string buf, int|void offset) {
    int idx = offset || 0;
    int len = sizeof(buf);
    int multiplier = 1;
    int value = 0;
    int bytes_consumed = 0;

    while (1) {
        if (idx >= len) {
            throw(Binnacle.Errors.MalformedPacketError(
                "Truncated variable byte integer in packet stream",
                Binnacle.Constants.RC_MALFORMED_PACKET
            ));
        }

        int byte_val = buf[idx++];
        bytes_consumed++;

        value += (byte_val & 0x7F) * multiplier;
        if (multiplier > 128 * 128 * 128) {
            throw(Binnacle.Errors.MalformedPacketError(
                "Variable byte integer continuation bit exceeded 4 bytes",
                Binnacle.Constants.RC_MALFORMED_PACKET
            ));
        }

        multiplier *= 128;
        if ((byte_val & 0x80) == 0) {
            break;
        }
    }

    return ({ value, bytes_consumed });
}

//! Return the byte length required to encode value as a variable-byte integer.
int encoded_length(int val) {
    if (val <= 127) return 1;
    if (val <= 16383) return 2;
    if (val <= 2097151) return 3;
    if (val <= MAX_VARINT_VALUE) return 4;
    return 0;
}
