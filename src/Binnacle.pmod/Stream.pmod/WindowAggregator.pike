// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Tumbling and Sliding Time-Window Streaming Aggregators

string window_type; // "tumbling" or "sliding"
int window_seconds;
array(mapping(string:mixed)) observations; // ({ ([ "val": float, "ts": int ]) })
Thread.Mutex agg_mutex;

void create(string|void _type, int|void _seconds) {
    window_type = _type || "tumbling";
    window_seconds = zero_type(_seconds) ? 10 : _seconds;
    observations = ({});
    agg_mutex = Thread.Mutex();
}

//! Record a numerical observation into the window.
void add_sample(float val, int|void ts) {
    Thread.MutexKey key = agg_mutex->lock();
    int sample_ts = zero_type(ts) ? time() : ts;
    observations += ({ ([ "val": (float)val, "ts": sample_ts ]) });
    _prune_expired(sample_ts);
    destruct(key);
}

//! Compute aggregation statistics over current window buffer.
mapping(string:float) compute_metrics() {
    Thread.MutexKey key = agg_mutex->lock();
    _prune_expired(time());

    int count = sizeof(observations);
    if (count == 0) {
        destruct(key);
        return ([ "count": 0.0, "avg": 0.0, "min": 0.0, "max": 0.0, "sum": 0.0, "stddev": 0.0 ]);
    }

    float sum = 0.0;
    float min_val = observations[0]->val;
    float max_val = observations[0]->val;

    foreach (observations, mapping obs) {
        float v = obs->val;
        sum += v;
        if (v < min_val) min_val = v;
        if (v > max_val) max_val = v;
    }

    float avg = sum / (float)count;

    // Standard deviation
    float var_sum = 0.0;
    foreach (observations, mapping obs) {
        float diff = obs->val - avg;
        var_sum += diff * diff;
    }
    float stddev = sqrt(var_sum / (float)count);

    mapping(string:float) res = ([
        "count": (float)count,
        "sum": sum,
        "avg": avg,
        "min": min_val,
        "max": max_val,
        "stddev": stddev
    ]);

    destruct(key);
    return res;
}

private void _prune_expired(int now) {
    if (window_type == "sliding") {
        int cutoff = now - window_seconds;
        array(mapping) retained = ({});
        foreach (observations, mapping obs) {
            if (obs->ts >= cutoff) {
                retained += ({ obs });
            }
        }
        observations = retained;
    }
}

//! Clear all observations (used at tumbling window boundary).
void reset() {
    Thread.MutexKey key = agg_mutex->lock();
    observations = ({});
    destruct(key);
}
