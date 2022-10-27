// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// CRC32 Checksum Implementation (IEEE 802.3 Polynomial)

private array(int) crc_table = 0;

//! Initialize the 256-entry lookup table for IEEE 802.3 CRC-32.
private void init_table() {
    crc_table = allocate(256);
    int poly = 0xEDB88320;
    for (int i = 0; i < 256; i++) {
        int c = i;
        for (int j = 0; j < 8; j++) {
            if (c & 1) {
                c = poly ^ ((c >> 1) & 0x7FFFFFFF);
            } else {
                c = (c >> 1) & 0x7FFFFFFF;
            }
        }
        crc_table[i] = c & 0xFFFFFFFF;
    }
}

//! Calculate 32-bit unsigned CRC32 of the provided binary string buffer.
int calculate(string data, int|void prev_crc) {
    if (!crc_table) {
        init_table();
    }

    int c = (prev_crc == 0) ? 0xFFFFFFFF : (prev_crc ^ 0xFFFFFFFF);
    int len = sizeof(data);

    for (int i = 0; i < len; i++) {
        int byte_val = data[i];
        int table_idx = (c ^ byte_val) & 0xFF;
        c = (crc_table[table_idx] ^ ((c >> 8) & 0x00FFFFFF)) & 0xFFFFFFFF;
    }

    return (c ^ 0xFFFFFFFF) & 0xFFFFFFFF;
}

//! Calculate CRC32 and return as 4-byte big-endian binary string.
string calculate_binary(string data) {
    int crc = calculate(data);
    return sprintf("%4c", crc);
}

//! Verify that data matches expected 32-bit CRC.
int verify(string data, int expected_crc) {
    return calculate(data) == (expected_crc & 0xFFFFFFFF);
}
