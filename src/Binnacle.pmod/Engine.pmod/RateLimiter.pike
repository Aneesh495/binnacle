// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Token Bucket Rate Limiter and Traffic Shaper

int max_rate;     // Tokens added per second
int burst_limit;  // Maximum token capacity in bucket
float tokens;     // Current available tokens
int last_refill_us;
Thread.Mutex limiter_mutex;

void create(int _rate, int|void _burst) {
    max_rate = _rate;
    burst_limit = zero_type(_burst) ? _rate : _burst;
    tokens = (float)burst_limit;
    last_refill_us = Binnacle.Utils.Time.monotonic_us();
    limiter_mutex = Thread.Mutex();
}

//! Attempt to consume specified number of tokens (e.g. bytes or packet count).
//! Returns 1 if permitted, 0 if rate limit exceeded.
int consume(int count) {
    if (max_rate <= 0) return 1; // Rate limiting disabled

    Thread.MutexKey key = limiter_mutex->lock();
    int now_us = Binnacle.Utils.Time.monotonic_us();
    int elapsed_us = now_us - last_refill_us;

    if (elapsed_us > 0) {
        float tokens_to_add = (elapsed_us * (float)max_rate) / 1000000.0;
        tokens += tokens_to_add;
        if (tokens > (float)burst_limit) {
            tokens = (float)burst_limit;
        }
        last_refill_us = now_us;
    }

    if (tokens >= (float)count) {
        tokens -= (float)count;
        destruct(key);
        return 1;
    }

    destruct(key);
    return 0; // Throttle: insufficient tokens
}

//! Return current available tokens.
float get_available_tokens() {
    Thread.MutexKey key = limiter_mutex->lock();
    float t = tokens;
    destruct(key);
    return t;
}
