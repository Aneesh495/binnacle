// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// $SYS System Topic Telemetry Publisher

int start_time;
int update_interval;
mixed callout_id;
object broker_ref;

void create(object broker, int|void interval) {
    broker_ref = broker;
    update_interval = zero_type(interval) ? 10 : interval;
    start_time = time();
    callout_id = 0;
}

//! Start background periodic publishing of $SYS telemetry topics.
void start() {
    publish_sys_topics();
    if (update_interval > 0) {
        callout_id = call_out(timer_tick, update_interval);
    }
}

//! Stop periodic publishing.
void stop() {
    if (callout_id) {
        remove_call_out(callout_id);
        callout_id = 0;
    }
}

private void timer_tick() {
    publish_sys_topics();
    callout_id = call_out(timer_tick, update_interval);
}

//! Publish all operational metrics to their respective $SYS topics as retained messages.
void publish_sys_topics() {
    if (!broker_ref) return;

    int uptime_secs = time() - start_time;
    mapping(string:string) sys_map = ([
        "$SYS/broker/version": Binnacle.version(),
        "$SYS/broker/uptime": sprintf("%d", uptime_secs),
        "$SYS/broker/timestamp": Binnacle.Utils.Time.format_iso8601(),
        "$SYS/broker/clients/connected": sprintf("%d", broker_ref->get_connected_clients_count()),
        "$SYS/broker/clients/total": sprintf("%d", broker_ref->get_total_clients_count()),
        "$SYS/broker/subscriptions/count": sprintf("%d", broker_ref->get_subscriptions_count()),
        "$SYS/broker/messages/retained/count": sprintf("%d", broker_ref->get_retained_count()),
        "$SYS/broker/messages/received": sprintf("%d", broker_ref->get_messages_received_count()),
        "$SYS/broker/messages/sent": sprintf("%d", broker_ref->get_messages_sent_count()),
        "$SYS/broker/bytes/received": sprintf("%d", broker_ref->get_bytes_received_count()),
        "$SYS/broker/bytes/sent": sprintf("%d", broker_ref->get_bytes_sent_count())
    ]);

    foreach (indices(sys_map), string topic) {
        string payload = sys_map[topic];
        broker_ref->internal_publish(topic, payload, 0, 1);
    }
}
