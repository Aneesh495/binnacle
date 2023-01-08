// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Global Session Registry and Expiration Reaper

object broker;
mapping(string:object) sessions; // client_id -> Session
Thread.Mutex manager_mutex;

void create(object _broker) {
    broker = _broker;
    sessions = ([]);
    manager_mutex = Thread.Mutex();
}

//! Retrieve an existing session or initialize a new session adhering to CleanStart semantics.
//! Returns ({ object session, int session_present }).
array(mixed) get_or_create_session(string cid, int clean_start, int expiry_interval, int rx_max) {
    Thread.MutexKey key = manager_mutex->lock();

    int session_present = 0;
    object sess = 0;

    if (has_index(sessions, cid)) {
        object existing = sessions[cid];

        // Session takeover check: terminate prior active connection if still connected
        if (existing->is_active && existing->current_actor) {
            Binnacle.Utils.Logger.info("session", sprintf("Session takeover for client %s", cid));
            existing->current_actor->disconnect(
                Binnacle.Constants.RC_SESSION_TAKEN_OVER, "Session taken over by new connection"
            );
        }

        if (clean_start) {
            // CleanStart=1: Discard existing session state
            existing->clear();
            sess = existing;
            sess->session_expiry_interval = expiry_interval;
            session_present = 0;
        } else {
            // CleanStart=0: Resume existing session
            sess = existing;
            sess->session_expiry_interval = expiry_interval;
            session_present = 1;
        }
    } else {
        // Create new session
        sess = .Session(cid, expiry_interval, rx_max);
        sessions[cid] = sess;
        session_present = 0;
    }

    destruct(key);
    return ({ sess, session_present });
}

//! Retrieve session by client identifier.
object get_session(string cid) {
    Thread.MutexKey key = manager_mutex->lock();
    object sess = 0;
    if (has_index(sessions, cid)) {
        sess = sessions[cid];
    }
    destruct(key);
    return sess;
}

//! Manually terminate and delete a session.
void remove_session(string cid) {
    Thread.MutexKey key = manager_mutex->lock();
    if (has_index(sessions, cid)) {
        object sess = sessions[cid];
        sess->clear();
        m_delete(sessions, cid);
    }
    destruct(key);
}

//! Scan and garbage collect expired offline sessions.
void reap_expired_sessions(int now) {
    Thread.MutexKey key = manager_mutex->lock();
    array(string) expired_cids = ({});

    foreach (indices(sessions), string cid) {
        object sess = sessions[cid];
        if (sess->is_expired(now)) {
            expired_cids += ({ cid });
        }
    }

    foreach (expired_cids, string cid) {
        object sess = sessions[cid];
        sess->clear();
        m_delete(sessions, cid);
        Binnacle.Utils.Logger.info("session", sprintf("Reaped expired session for client %s", cid));
    }

    destruct(key);
}

//! Return total number of stored sessions.
int total_count() {
    Thread.MutexKey key = manager_mutex->lock();
    int count = sizeof(sessions);
    destruct(key);
    return count;
}

//! Return count of actively connected sessions.
int active_count() {
    Thread.MutexKey key = manager_mutex->lock();
    int count = 0;
    foreach (values(sessions), object sess) {
        if (sess->is_active) count++;
    }
    destruct(key);
    return count;
}

//! Export all sessions for snapshotting.
array(object) to_array() {
    Thread.MutexKey key = manager_mutex->lock();
    array(object) all = values(sessions);
    destruct(key);
    return all;
}
