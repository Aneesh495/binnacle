// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Last Will and Testament (LWT) Manager with Delayed Dispatch (MQTT 5.0 Section 3.1.2.5)

object broker;
mapping(string:object) active_wills; // client_id -> WillMessage
mapping(string:mapping(string:mixed)) pending_delayed_wills; // client_id -> ([ will: msg, due_time: int ])
Thread.Mutex will_mutex;

void create(object _broker) {
    broker = _broker;
    active_wills = ([]);
    pending_delayed_wills = ([]);
    will_mutex = Thread.Mutex();
}

//! Register a Will message for a client on CONNECT.
void register_will(string cid, object will_msg) {
    Thread.MutexKey key = will_mutex->lock();
    active_wills[cid] = will_msg;
    if (has_index(pending_delayed_wills, cid)) {
        m_delete(pending_delayed_wills, cid);
    }
    destruct(key);
}

//! Client reconnected: cancel any pending delayed will publication.
void on_client_reconnected(string cid) {
    Thread.MutexKey key = will_mutex->lock();
    if (has_index(pending_delayed_wills, cid)) {
        m_delete(pending_delayed_wills, cid);
        Binnacle.Utils.Logger.info("will", sprintf("Cancelled delayed will for reconnected client %s", cid));
    }
    destruct(key);
}

//! Client disconnected.
//! If unexpected == 1, triggers will dispatch or schedules delayed publication.
void on_client_disconnected(string cid, int unexpected) {
    Thread.MutexKey key = will_mutex->lock();

    if (!has_index(active_wills, cid) || !unexpected) {
        // Clean disconnect or no will registered: discard
        if (has_index(pending_delayed_wills, cid)) {
            m_delete(pending_delayed_wills, cid);
        }
        destruct(key);
        return;
    }

    object will = active_wills[cid];
    int delay = will->delay_interval;

    if (delay > 0) {
        int due = time() + delay;
        pending_delayed_wills[cid] = ([ "will": will, "due": due ]);
        Binnacle.Utils.Logger.info("will", sprintf("Scheduled delayed will for client %s in %d seconds", cid, delay));
        destruct(key);
        return;
    }

    // Immediate dispatch
    m_delete(active_wills, cid);
    destruct(key);

    _publish_will(will);
}

//! Periodically invoked by broker tick to dispatch matured delayed will messages.
void check_delayed_wills(int now) {
    Thread.MutexKey key = will_mutex->lock();
    array(string) matured = ({});

    foreach (indices(pending_delayed_wills), string cid) {
        mapping entry = pending_delayed_wills[cid];
        if (now >= entry->due) {
            matured += ({ cid });
        }
    }

    array(object) to_dispatch = ({});
    foreach (matured, string cid) {
        to_dispatch += ({ pending_delayed_wills[cid]->will });
        m_delete(pending_delayed_wills, cid);
        m_delete(active_wills, cid);
    }
    destruct(key);

    foreach (to_dispatch, object will) {
        _publish_will(will);
    }
}

//! Dispatch a will message through broker routing fabric.
private void _publish_will(object will) {
    Binnacle.Utils.Logger.info("will", sprintf("Dispatching Last Will to topic %s", will->topic));
    broker->internal_publish(will->topic, will->payload, will->qos, will->retain, will->properties);
}

//! Discard client will on explicit unregistration.
void unregister_will(string cid) {
    Thread.MutexKey key = will_mutex->lock();
    m_delete(active_wills, cid);
    m_delete(pending_delayed_wills, cid);
    destruct(key);
}
