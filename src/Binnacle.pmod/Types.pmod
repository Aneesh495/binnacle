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
                int|void _delay, mapping(int:mixed)|void _props) {
        topic = _topic;
        payload = _payload;
        qos = _qos;
        retain = _retain;
        delay_interval = zero_type(_delay) ? 0 : _delay;
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
                mapping(int:mixed)|void _props, int|void _ts) {
        topic = _topic;
        payload = _payload;
        qos = _qos;
        properties = _props || ([]);
        timestamp = zero_type(_ts) ? time() : _ts;
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
                int|void _nl, int|void _rap, int|void _rh, int|void _sub_id) {
        topic_filter = _filter;
        qos = _qos;
        client_id = _client_id;
        no_local = zero_type(_nl) ? 0 : _nl;
        retain_as_published = zero_type(_rap) ? 0 : _rap;
        retain_handling = zero_type(_rh) ? 0 : _rh;
        subscription_identifier = zero_type(_sub_id) ? 0 : _sub_id;
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
                mapping(int:mixed)|void _props) {
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
