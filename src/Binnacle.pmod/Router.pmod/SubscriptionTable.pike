// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Client Subscription Reverse Index Table

mapping(string:mapping(string:object)) client_subs; // client_id -> (filter -> Subscription)
Thread.Mutex table_mutex;

void create() {
    client_subs = ([]);
    table_mutex = Thread.Mutex();
}

//! Register a subscription in the reverse lookup index.
void add(object sub) {
    Thread.MutexKey key = table_mutex->lock();
    string cid = sub->client_id;
    if (!has_index(client_subs, cid)) {
        client_subs[cid] = ([]);
    }
    client_subs[cid][sub->topic_filter] = sub;
    destruct(key);
}

//! Remove a specific subscription filter for a client.
object remove(string cid, string filter) {
    Thread.MutexKey key = table_mutex->lock();
    object sub = 0;
    if (has_index(client_subs, cid)) {
        if (has_index(client_subs[cid], filter)) {
            sub = client_subs[cid][filter];
            m_delete(client_subs[cid], filter);
        }
        if (sizeof(client_subs[cid]) == 0) {
            m_delete(client_subs, cid);
        }
    }
    destruct(key);
    return sub;
}

//! Remove all subscriptions associated with a client.
//! Useful on session termination or CleanStart=1.
array(object) remove_all(string cid) {
    Thread.MutexKey key = table_mutex->lock();
    array(object) subs = ({});
    if (has_index(client_subs, cid)) {
        subs = values(client_subs[cid]);
        m_delete(client_subs, cid);
    }
    destruct(key);
    return subs;
}

//! Retrieve all subscriptions for a client.
array(object) get_all(string cid) {
    Thread.MutexKey key = table_mutex->lock();
    array(object) subs = ({});
    if (has_index(client_subs, cid)) {
        subs = values(client_subs[cid]);
    }
    destruct(key);
    return subs;
}

//! Count number of active subscriptions for a client.
int count_for_client(string cid) {
    Thread.MutexKey key = table_mutex->lock();
    int count = 0;
    if (has_index(client_subs, cid)) {
        count = sizeof(client_subs[cid]);
    }
    destruct(key);
    return count;
}

//! Return total number of subscriptions across all clients.
int total_subscriptions() {
    Thread.MutexKey key = table_mutex->lock();
    int total = 0;
    foreach (values(client_subs), mapping m) {
        total += sizeof(m);
    }
    destruct(key);
    return total;
}
