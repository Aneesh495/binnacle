/*
 * Binnacle: Distributed MQTT 5.0 Telemetry Broker
 * File: c_src/accel/binnacle_accel.h
 * Description: Header definitions for native C acceleration routines.
 */

#ifndef BINNACLE_ACCEL_H
#define BINNACLE_ACCEL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Encode a 32-bit integer into MQTT 5.0 Variable Byte Integer format.
 * Returns 0 on success, or non-zero if buffer is too small or value exceeds 268,435,455.
 */
int binnacle_varint_encode(uint32_t val, uint8_t *buf, size_t max_len, size_t *out_len);

/*
 * Decode MQTT 5.0 Variable Byte Integer from buffer.
 * Returns 0 on success, or non-zero on malformed data or incomplete buffer.
 */
int binnacle_varint_decode(const uint8_t *buf, size_t len, uint32_t *out_val, size_t *bytes_read);

/*
 * Fast C topic filter matching against published topic name.
 * Evaluates '+' and '#' wildcards following OASIS MQTT specification.
 * Returns true (1) if topic matches filter, false (0) otherwise.
 */
bool binnacle_topic_matches(const char *topic, const char *filter);

/*
 * Hardware-optimized IEEE 802.3 CRC32 calculation with loop unrolling.
 */
uint32_t binnacle_crc32_fast(const uint8_t *buf, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* BINNACLE_ACCEL_H */
