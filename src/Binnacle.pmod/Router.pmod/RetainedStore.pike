// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Retained Message Store and Topic Matching Engine

mapping(string:object) retained_messages; // topic -> RetainedMessage
Thread.Mutex store_mutex;

void create() {
    retained_messages = ([]);
    store_mutex = Thread.Mutex();
}

//! Store or delete a retained message.
//! An empty payload signals deletion of the retained message on that topic.
int put(string topic, string payload, int qos, mapping(int:mixed)|void props) {
    Thread.MutexKey key = store_mutex->lock();

    if (!payload || sizeof(payload) == 0) {
        // Zero-length payload deletes existing retained message
        if (has_index(retained_messages, topic)) {
            m_delete(retained_messages, topic);
            destruct(key);
            return -1; // Deleted
        }
        destruct(key);
        return 0;
    }

    object msg = Binnacle.Types.RetainedMessage(topic, payload, qos, props);
    retained_messages[topic] = msg;
    destruct(key);
    return 1; // Stored
}

//! Retrieve retained message for exact topic.
object get(string topic) {
    Thread.MutexKey key = store_mutex->lock();
    object msg = 0;
    if (has_index(retained_messages, topic)) {
        msg = retained_messages[topic];
    }
    destruct(key);
    return msg;
}

//! Delete retained message on topic.
int delete_topic(string topic) {
    Thread.MutexKey key = store_mutex->lock();
    int removed = 0;
    if (has_index(retained_messages, topic)) {
        m_delete(retained_messages, topic);
        removed = 1;
    }
    destruct(key);
    return removed;
}

//! Find all retained messages that match a subscription topic filter.
array(object) match(string filter) {
    Thread.MutexKey key = store_mutex->lock();
    array(object) matched = ({});

    foreach (values(retained_messages), object msg) {
        if (_topic_matches_filter(msg->topic, filter)) {
            matched += ({ msg });
        }
    }

    destruct(key);
    return matched;
}

//! Validate if an exact topic matches a filter with wildcards.
private int _topic_matches_filter(string topic, string filter) {
    if (topic == filter) return 1;
    if (filter == "#") {
        // Root '#' does not match '$' system topics
        return !(sizeof(topic) > 0 && topic[0] == '$');
    }

    array(string) top_segs = topic / "/";
    array(string) flt_segs = filter / "/";

    int top_len = sizeof(top_segs);
    int flt_len = sizeof(flt_segs);

    int i = 0;
    while (i < flt_len) {
        string f_seg = flt_segs[i];

        if (f_seg == "#") {
            // Must be at end; matches everything remaining
            return 1;
        }

        if (i >= top_len) {
            return 0; // Filter longer than topic
        }

        string t_seg = top_segs[i];
        if (f_seg != "+" && f_seg != t_seg) {
            return 0;
        }

        i++;
    }

    return i == top_len;
}

//! Return total count of retained messages in store.
int count() {
    Thread.MutexKey key = store_mutex->lock();
    int c = sizeof(retained_messages);
    destruct(key);
    return c;
}

//! Export all retained messages as an array for snapshotting.
array(object) to_array() {
    Thread.MutexKey key = store_mutex->lock();
    array(object) all = values(retained_messages);
    destruct(key);
    return all;
}
