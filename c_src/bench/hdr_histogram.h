/*
 * Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
 * High Dynamic Range (HDR) Latency Histogram Interface
 */

#ifndef BINNACLE_HDR_HISTOGRAM_H
#define BINNACLE_HDR_HISTOGRAM_H

#include <stdint.h>
#include <stddef.h>

#define HDR_MAX_VALUE 60000000000ULL /* 60 seconds in nanoseconds */
#define HDR_BUCKET_COUNT 64

typedef struct {
    uint64_t total_count;
    uint64_t min_value;
    uint64_t max_value;
    double sum_values;
    uint64_t counts[1024];
    uint64_t sub_bucket_mask;
    int sub_bucket_count;
} hdr_histogram_t;

int hdr_init(hdr_histogram_t* hist);
void hdr_record(hdr_histogram_t* hist, uint64_t value);
uint64_t hdr_value_at_percentile(const hdr_histogram_t* hist, double percentile);
double hdr_mean(const hdr_histogram_t* hist);
void hdr_reset(hdr_histogram_t* hist);

#endif /* BINNACLE_HDR_HISTOGRAM_H */
