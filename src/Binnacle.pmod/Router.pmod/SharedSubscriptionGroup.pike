// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Shared Subscription Group and Load Balancing Dispatcher

string group_name;
string strategy; // "round_robin", "sticky_hash", "random"
mapping(string:object) subscribers; // client_id -> Subscription
array(string) subscriber_order;
int round_robin_index;

void create(string _name, string|void _strategy) {
    group_name = _name;
    strategy = _strategy || "round_robin";
    subscribers = ([]);
    subscriber_order = ({});
    round_robin_index = 0;
}

//! Register a subscriber in this shared group.
void add_subscriber(object sub) {
    string cid = sub->client_id;
    if (!has_index(subscribers, cid)) {
        subscriber_order += ({ cid });
    }
    subscribers[cid] = sub;
}

//! Remove a subscriber from this shared group.
void remove_subscriber(string cid) {
    if (has_index(subscribers, cid)) {
        m_delete(subscribers, cid);
        subscriber_order -= ({ cid });
        if (round_robin_index >= sizeof(subscriber_order)) {
            round_robin_index = 0;
        }
    }
}

//! Select one subscriber from the group based on configured strategy.
object select_subscriber(string topic, string|void publisher_cid) {
    int total = sizeof(subscriber_order);
    if (total == 0) return 0;

    if (strategy == "random") {
        int idx = random(total);
        string cid = subscriber_order[idx];
        return subscribers[cid];
    } else if (strategy == "sticky_hash") {
        // Hash topic or publisher client ID for consistent affinity
        string key = publisher_cid || topic;
        int h = hash_value(key);
        if (h < 0) h = -h;
        int idx = h % total;
        string cid = subscriber_order[idx];
        return subscribers[cid];
    } else {
        // Round-robin default
        if (round_robin_index >= total) {
            round_robin_index = 0;
        }
        string cid = subscriber_order[round_robin_index];
        round_robin_index = (round_robin_index + 1) % total;
        return subscribers[cid];
    }
}

//! Check if group has zero subscribers.
int is_empty() {
    return sizeof(subscriber_order) == 0;
}
