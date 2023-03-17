/*
 * Binnacle: Distributed MQTT 5.0 Telemetry Broker
 * File: c_src/accel/test_accel.c
 * Description: Unit tests for native C acceleration routines.
 */

#include "binnacle_accel.h"
#include <stdio.h>
#include <assert.h>
#include <string.h>

static void test_varint(void) {
    uint8_t buf[8];
    size_t out_len = 0;
    uint32_t decoded = 0;
    size_t bytes_read = 0;

    /* 0 */
    assert(binnacle_varint_encode(0, buf, sizeof(buf), &out_len) == 0);
    assert(out_len == 1 && buf[0] == 0x00);
    assert(binnacle_varint_decode(buf, out_len, &decoded, &bytes_read) == 0);
    assert(decoded == 0 && bytes_read == 1);

    /* 127 */
    assert(binnacle_varint_encode(127, buf, sizeof(buf), &out_len) == 0);
    assert(out_len == 1 && buf[0] == 0x7F);
    assert(binnacle_varint_decode(buf, out_len, &decoded, &bytes_read) == 0);
    assert(decoded == 127 && bytes_read == 1);

    /* 128 */
    assert(binnacle_varint_encode(128, buf, sizeof(buf), &out_len) == 0);
    assert(out_len == 2 && buf[0] == 0x80 && buf[1] == 0x01);
    assert(binnacle_varint_decode(buf, out_len, &decoded, &bytes_read) == 0);
    assert(decoded == 128 && bytes_read == 2);

    /* 268435455 (Max 4-byte MQTT value) */
    assert(binnacle_varint_encode(268435455U, buf, sizeof(buf), &out_len) == 0);
    assert(out_len == 4);
    assert(binnacle_varint_decode(buf, out_len, &decoded, &bytes_read) == 0);
    assert(decoded == 268435455U && bytes_read == 4);

    /* Overflow */
    assert(binnacle_varint_encode(268435456U, buf, sizeof(buf), &out_len) == -2);

    printf("  [PASS] C VarInt codec tests\n");
}

static void test_topic_matching(void) {
    /* Exact matches */
    assert(binnacle_topic_matches("sensor/temp", "sensor/temp") == true);
    assert(binnacle_topic_matches("sensor/temp", "sensor/pressure") == false);

    /* Single-level wildcard '+' */
    assert(binnacle_topic_matches("sensor/temp", "sensor/+") == true);
    assert(binnacle_topic_matches("sensor/temp/living", "sensor/+") == false);
    assert(binnacle_topic_matches("sensor/temp/living", "sensor/+/living") == true);
    assert(binnacle_topic_matches("sensor/temp/kitchen", "sensor/+/living") == false);

    /* Multi-level wildcard '#' */
    assert(binnacle_topic_matches("sensor/temp", "#") == true);
    assert(binnacle_topic_matches("sensor/temp/living", "sensor/#") == true);
    assert(binnacle_topic_matches("device/01/status/firmware", "device/#") == true);

    /* $SYS isolation */
    assert(binnacle_topic_matches("$SYS/broker/uptime", "#") == false);
    assert(binnacle_topic_matches("$SYS/broker/uptime", "+/broker/uptime") == false);
    assert(binnacle_topic_matches("$SYS/broker/uptime", "$SYS/#") == true);

    printf("  [PASS] C Topic matching tests\n");
}

static void test_crc32(void) {
    const char *data = "123456789";
    uint32_t crc = binnacle_crc32_fast((const uint8_t *)data, strlen(data));
    assert(crc == 0xCBF43926U);

    printf("  [PASS] C CRC32 fast calculation tests\n");
}

int main(void) {
    printf("Starting Native C Acceleration Test Suite...\n");
    test_varint();
    test_topic_matching();
    test_crc32();
    printf("All Native C Acceleration tests passed successfully.\n");
    return 0;
}
