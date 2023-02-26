// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Metrics Registry and OpenMetrics Container

mapping(string:int) counters;
mapping(string:float) gauges;
mapping(string:object) histograms; // name -> LatencyHistogram
Thread.Mutex registry_mutex;

void create() {
    counters = ([]);
    gauges = ([]);
    histograms = ([]);
    registry_mutex = Thread.Mutex();
}

//! Increment a counter metric.
void counter_inc(string name, int|void amount) {
    int amt = zero_type(amount) ? 1 : amount;
    Thread.MutexKey key = registry_mutex->lock();
    counters[name] += amt;
    destruct(key);
}

//! Set a gauge metric.
void gauge_set(string name, float val) {
    Thread.MutexKey key = registry_mutex->lock();
    gauges[name] = (float)val;
    destruct(key);
}

//! Record observation in a named histogram.
void histogram_observe(string name, int duration_us) {
    Thread.MutexKey key = registry_mutex->lock();
    if (!has_index(histograms, name)) {
        histograms[name] = .LatencyHistogram();
    }
    object h = histograms[name];
    destruct(key);

    h->record(duration_us);
}

//! Retrieve histogram by name.
object get_histogram(string name) {
    Thread.MutexKey key = registry_mutex->lock();
    object h = has_index(histograms, name) ? histograms[name] : 0;
    destruct(key);
    return h;
}

//! Snapshot of all registered metrics.
mapping(string:mixed) get_all_metrics() {
    Thread.MutexKey key = registry_mutex->lock();
    mapping snapshot = ([
        "counters": copy_value(counters),
        "gauges": copy_value(gauges),
        "histograms": ([])
    ]);

    foreach (indices(histograms), string h_name) {
        snapshot->histograms[h_name] = histograms[h_name]->get_summary();
    }

    destruct(key);
    return snapshot;
}
