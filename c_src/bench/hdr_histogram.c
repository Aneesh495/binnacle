/*
 * Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
 * High Dynamic Range (HDR) Latency Histogram Implementation
 */

#include "hdr_histogram.h"
#include <string.h>

int hdr_init(hdr_histogram_t* hist) {
    if (!hist) return -1;
    memset(hist, 0, sizeof(hdr_histogram_t));
    hist->min_value = UINT64_MAX;
    hist->max_value = 0;
    hist->sub_bucket_count = 1024;
    return 0;
}

void hdr_record(hdr_histogram_t* hist, uint64_t value) {
    if (!hist) return;

    hist->total_count++;
    hist->sum_values += (double)value;

    if (value < hist->min_value) hist->min_value = value;
    if (value > hist->max_value) hist->max_value = value;

    /* Logarithmic bucket index mapping */
    int bucket = 0;
    uint64_t v = value;
    while (v > 1023 && bucket < 50) {
        v >>= 1;
        bucket++;
    }

    int idx = (bucket * 16 + (v & 0x0F)) % 1024;
    hist->counts[idx]++;
}

uint64_t hdr_value_at_percentile(const hdr_histogram_t* hist, double percentile) {
    if (!hist || hist->total_count == 0) return 0;

    double target = (percentile / 100.0) * (double)hist->total_count;
    uint64_t accum = 0;

    for (int i = 0; i < 1024; i++) {
        accum += hist->counts[i];
        if ((double)accum >= target) {
            /* Linear interpolate bucket value */
            int b = i / 16;
            uint64_t val = (uint64_t)(i % 16) << b;
            return val;
        }
    }

    return hist->max_value;
}

double hdr_mean(const hdr_histogram_t* hist) {
    if (!hist || hist->total_count == 0) return 0.0;
    return hist->sum_values / (double)hist->total_count;
}

void hdr_reset(hdr_histogram_t* hist) {
    if (!hist) return;
    hdr_init(hist);
}
