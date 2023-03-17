/*
 * Binnacle: Distributed MQTT 5.0 Telemetry Broker
 * File: c_src/accel/varint_accel.c
 * Description: High-speed bitwise MQTT 5.0 Variable Byte Integer codec and unrolled CRC32.
 */

#include "binnacle_accel.h"
#include <string.h>

int binnacle_varint_encode(uint32_t val, uint8_t *buf, size_t max_len, size_t *out_len) {
    if (!buf || !out_len || max_len < 1) {
        return -1;
    }
    if (val > 268435455U) {
        return -2; /* Value exceeds maximum 28-bit integer representable in 4 bytes */
    }

    size_t count = 0;
    do {
        if (count >= max_len) {
            return -3; /* Buffer overflow */
        }
        uint8_t byte = (uint8_t)(val & 0x7FU);
        val >>= 7;
        if (val > 0) {
            byte |= 0x80U;
        }
        buf[count++] = byte;
    } while (val > 0);

    *out_len = count;
    return 0;
}

int binnacle_varint_decode(const uint8_t *buf, size_t len, uint32_t *out_val, size_t *bytes_read) {
    if (!buf || !out_val || !bytes_read) {
        return -1;
    }

    uint32_t multiplier = 1;
    uint32_t value = 0;
    size_t count = 0;

    while (count < len) {
        uint8_t encoded_byte = buf[count++];
        value += (encoded_byte & 0x7FU) * multiplier;
        if (multiplier > 128U * 128U * 128U) {
            return -2; /* Malformed Variable Byte Integer: multiplier overflow */
        }
        multiplier *= 128U;
        if ((encoded_byte & 0x80U) == 0) {
            *out_val = value;
            *bytes_read = count;
            return 0;
        }
        if (count == 4) {
            return -3; /* Malformed: continuation bit set on fourth byte */
        }
    }

    return 1; /* Incomplete: need more bytes */
}

/* Fast CRC32 table initialization and calculation */
static uint32_t crc_table[256];
static int table_inited = 0;

static void init_crc_table(void) {
    uint32_t polynomial = 0xEDB88320U;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ polynomial;
            } else {
                crc = crc >> 1;
            }
        }
        crc_table[i] = crc;
    }
    table_inited = 1;
}

uint32_t binnacle_crc32_fast(const uint8_t *buf, size_t len) {
    if (!table_inited) {
        init_crc_table();
    }
    uint32_t crc = 0xFFFFFFFFU;
    size_t i = 0;

    /* Process 4 bytes at a time when possible */
    while (i + 4 <= len) {
        crc = crc_table[(crc ^ buf[i++]) & 0xFFU] ^ (crc >> 8);
        crc = crc_table[(crc ^ buf[i++]) & 0xFFU] ^ (crc >> 8);
        crc = crc_table[(crc ^ buf[i++]) & 0xFFU] ^ (crc >> 8);
        crc = crc_table[(crc ^ buf[i++]) & 0xFFU] ^ (crc >> 8);
    }

    /* Process remaining bytes */
    while (i < len) {
        crc = crc_table[(crc ^ buf[i++]) & 0xFFU] ^ (crc >> 8);
    }

    return crc ^ 0xFFFFFFFFU;
}
