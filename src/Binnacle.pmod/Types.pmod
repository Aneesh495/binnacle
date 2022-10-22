// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Core Type Definitions and Data Containers

//! Encapsulates a key-value user property as specified in MQTT 5.0.
class UserProperty {
    string key;
    string val;

    void create(string _key, string _val) {
        key = _key;
        val = _val;
    }

    string encode() {
        return sprintf("%2c%s%2c%s", sizeof(key), key, sizeof(val), val);
    }
}

//! Encapsulates a Last Will and Testament definition.
class WillMessage {
    string topic;
    string payload;
    int qos;
    int retain;
    int delay_interval;
    mapping(int:mixed) properties;

    void create(string _topic, string _payload, int _qos, int _retain,
                int _delay = 0, mapping(int:mixed)|void _props = 0) {
        topic = _topic;
        payload = _payload;
        qos = _qos;
        retain = _retain;
        delay_interval = _delay;
        properties = _props || ([]);
    }
}

//! Represents a retained message stored on a specific topic.
class RetainedMessage {
    string topic;
    string payload;
    int qos;
    int timestamp;
    mapping(int:mixed) properties;

    void create(string _topic, string _payload, int _qos,
                mapping(int:mixed)|void _props = 0, int|void _ts = 0) {
        topic = _topic;
        payload = _payload;
        qos = _qos;
        properties = _props || ([]);
        timestamp = _ts || time();
    }
}

//! Represents an individual subscription entry registered by a client.
class SubscriptionEntry {
    string topic_filter;
    int qos;
    int no_local;
    int retain_as_published;
    int retain_handling;
    int subscription_identifier;
    string client_id;

    void create(string _filter, int _qos, string _client_id,
                int _nl = 0, int _rap = 0, int _rh = 0, int _sub_id = 0) {
        topic_filter = _filter;
        qos = _qos;
        client_id = _client_id;
        no_local = _nl;
        retain_as_published = _rap;
        retain_handling = _rh;
        subscription_identifier = _sub_id;
    }
}

//! Represents an inflight message awaiting acknowledgment.
class InflightMessage {
    int packet_id;
    int qos;
    string topic;
    string payload;
    int state; // 0: published, 1: pubrec received, 2: pubrel sent
    int timestamp;
    int retry_count;
    mapping(int:mixed) properties;

    void create(int _pid, int _qos, string _topic, string _payload,
                mapping(int:mixed)|void _props = 0) {
        packet_id = _pid;
        qos = _qos;
        topic = _topic;
        payload = _payload;
        properties = _props || ([]);
        state = 0;
        timestamp = time();
        retry_count = 0;
    }
}
