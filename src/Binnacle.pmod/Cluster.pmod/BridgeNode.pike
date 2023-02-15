// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Distributed Bridge Node for Inter-Broker Topic Forwarding

object broker;
string remote_host;
int remote_port;
string direction; // "in", "out", "both"
array(mapping(string:string)) topic_rules; // ({ ([ "local": "sensors/#", "remote": "edge/node-1/sensors/#" ]) })
object client_actor;
int is_active;
mixed retry_callout;
Thread.Mutex bridge_mutex;

void create(object _broker, string _host, int _port, string|void _dir) {
    broker = _broker;
    remote_host = _host;
    remote_port = _port;
    direction = _dir || "out";
    topic_rules = ({});
    client_actor = 0;
    is_active = 0;
    retry_callout = 0;
    bridge_mutex = Thread.Mutex();
}

//! Register a topic mapping pattern for bridging.
void add_topic_mapping(string local_filter, string remote_prefix) {
    Thread.MutexKey key = bridge_mutex->lock();
    topic_rules += ({ ([
        "local": local_filter,
        "remote": remote_prefix
    ]) });
    destruct(key);
}

//! Check if a locally published topic matches bridge forwarding rules.
string|int map_local_to_remote(string topic) {
    Thread.MutexKey key = bridge_mutex->lock();
    string target = 0;

    foreach (topic_rules, mapping rule) {
        if (rule->local == "#" || rule->local == topic || has_prefix(topic, rule->local[0..sizeof(rule->local)-2])) {
            target = rule->remote + topic;
            break;
        }
    }

    destruct(key);
    return target;
}
