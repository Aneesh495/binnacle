// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Microsecond Latency Histogram and Percentile Calculator

array(int) bucket_limits; // In microseconds
array(int) bucket_counts;
int total_count;
float total_sum_us;
int min_us;
int max_us;
Thread.Mutex hist_mutex;

void create(array(int)|void custom_buckets) {
    bucket_limits = custom_buckets || ({
        10, 25, 50, 100, 250, 500,
        1000, 2500, 5000, 10000, 25000, 50000,
        100000, 250000, 500000, 1000000, 5000000
    });
    bucket_counts = allocate(sizeof(bucket_limits) + 1); // Extra bucket for > max
    total_count = 0;
    total_sum_us = 0.0;
    min_us = 2147483647;
    max_us = 0;
    hist_mutex = Thread.Mutex();
}

//! Record a duration observation in microseconds.
void record(int duration_us) {
    if (duration_us < 0) duration_us = 0;

    Thread.MutexKey key = hist_mutex->lock();
    total_count++;
    total_sum_us += (float)duration_us;

    if (duration_us < min_us) min_us = duration_us;
    if (duration_us > max_us) max_us = duration_us;

    int placed = 0;
    for (int i = 0; i < sizeof(bucket_limits); i++) {
        if (duration_us <= bucket_limits[i]) {
            bucket_counts[i]++;
            placed = 1;
            break;
        }
    }
    if (!placed) {
        bucket_counts[-1]++; // Overflow bucket
    }

    destruct(key);
}

//! Compute approximate percentile (e.g. 50.0 for p50, 99.0 for p99).
float get_percentile(float pct) {
    Thread.MutexKey key = hist_mutex->lock();
    if (total_count == 0) {
        destruct(key);
        return 0.0;
    }

    float target_count = (pct / 100.0) * (float)total_count;
    int accumulated = 0;

    for (int i = 0; i < sizeof(bucket_limits); i++) {
        accumulated += bucket_counts[i];
        if ((float)accumulated >= target_count) {
            float p_val = (float)bucket_limits[i];
            destruct(key);
            return p_val;
        }
    }

    float m_val = (float)max_us;
    destruct(key);
    return m_val;
}

//! Return summary statistics snapshot.
mapping(string:float) get_summary() {
    Thread.MutexKey key = hist_mutex->lock();
    if (total_count == 0) {
        destruct(key);
        return ([
            "count": 0.0,
            "avg_us": 0.0,
            "p50_us": 0.0,
            "p90_us": 0.0,
            "p95_us": 0.0,
            "p99_us": 0.0,
            "max_us": 0.0
        ]);
    }

    float count_f = (float)total_count;
    float avg = total_sum_us / count_f;
    destruct(key);

    return ([
        "count": count_f,
        "avg_us": avg,
        "p50_us": get_percentile(50.0),
        "p90_us": get_percentile(90.0),
        "p95_us": get_percentile(95.0),
        "p99_us": get_percentile(99.0),
        "max_us": (float)max_us
    ]);
}
