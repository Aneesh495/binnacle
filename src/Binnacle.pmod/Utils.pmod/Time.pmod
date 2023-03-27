// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Time Utilities and Monotonic High-Resolution Clock

//! Return wall-clock time in seconds.
int now_seconds() {
    return time();
}

//! Return high-precision monotonic nanoseconds.
int monotonic_ns() {
    return gethrtime() * 1000;
}

//! Return high-precision monotonic microseconds.
int monotonic_us() {
    return gethrtime();
}

//! Return high-precision monotonic milliseconds.
int monotonic_ms() {
    return (int)(gethrtime() / 1000);
}

//! Format epoch timestamp into ISO 8601 UTC string.
string format_iso8601(int|void epoch_sec) {
    int ts = epoch_sec || time();
    mapping tm = gmtime(ts);
    return sprintf("%04d-%02d-%02dT%02d:%02d:%02dZ",
                   tm->year + 1900, tm->mon + 1, tm->mday,
                   tm->hour, tm->min, tm->sec);
}

//! Format duration in milliseconds into a concise human-readable string.
string format_duration(int ms) {
    if (ms < 1000) {
        return sprintf("%dms", ms);
    } else if (ms < 60000) {
        return sprintf("%.2fs", ms / 1000.0);
    } else if (ms < 3600000) {
        return sprintf("%.1fm", ms / 60000.0);
    } else {
        return sprintf("%.1fh", ms / 3600000.0);
    }
}

//! Check if a given future expiry timestamp has lapsed.
int is_expired(int expiry_ts) {
    if (expiry_ts <= 0) return 0;
    return time() >= expiry_ts;
}
