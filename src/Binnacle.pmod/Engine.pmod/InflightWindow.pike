// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Inflight Message Window and Flow Control Throttler (OASIS MQTT 5.0 Section 3.1.2.11.3)

int receive_maximum;
mapping(int:object) window; // packet_id -> InflightMessage
Thread.Mutex window_mutex;

void create(int|void _max) {
    receive_maximum = zero_type(_max) ? Binnacle.Constants.DEFAULT_RECEIVE_MAXIMUM : _max;
    if (receive_maximum <= 0) {
        receive_maximum = Binnacle.Constants.DEFAULT_RECEIVE_MAXIMUM;
    }
    window = ([]);
    window_mutex = Thread.Mutex();
}

//! Check if inflight window has reached the peer's ReceiveMaximum limit.
int is_full() {
    Thread.MutexKey key = window_mutex->lock();
    int full = sizeof(window) >= receive_maximum;
    destruct(key);
    return full;
}

//! Register an outgoing QoS 1 or QoS 2 message in the inflight window.
//! Throws QuotaExceededError if window limit is exceeded.
void add(object msg) {
    Thread.MutexKey key = window_mutex->lock();
    if (sizeof(window) >= receive_maximum) {
        destruct(key);
        throw(Binnacle.Errors.QuotaExceededError(
            sprintf("Inflight window capacity %d exceeded", receive_maximum),
            Binnacle.Constants.RC_RECEIVE_MAXIMUM_EXCEEDED
        ));
    }
    window[msg->packet_id] = msg;
    destruct(key);
}

//! Acknowledge and remove an inflight message by packet identifier.
object acknowledge(int pid) {
    Thread.MutexKey key = window_mutex->lock();
    object msg = 0;
    if (has_index(window, pid)) {
        msg = window[pid];
        m_delete(window, pid);
    }
    destruct(key);
    return msg;
}

//! Update state of an inflight message (e.g. from PUBREC received to PUBREL sent).
int update_state(int pid, int new_state) {
    Thread.MutexKey key = window_mutex->lock();
    int updated = 0;
    if (has_index(window, pid)) {
        window[pid]->state = new_state;
        window[pid]->timestamp = time();
        updated = 1;
    }
    destruct(key);
    return updated;
}

//! Retrieve inflight message by packet identifier.
object get(int pid) {
    Thread.MutexKey key = window_mutex->lock();
    object msg = 0;
    if (has_index(window, pid)) {
        msg = window[pid];
    }
    destruct(key);
    return msg;
}

//! Scan for unacknowledged messages whose timeout has expired.
array(object) get_timed_out_messages(int timeout_seconds) {
    Thread.MutexKey key = window_mutex->lock();
    array(object) timed_out = ({});
    int now = time();

    foreach (values(window), object msg) {
        if ((now - msg->timestamp) >= timeout_seconds) {
            timed_out += ({ msg });
        }
    }

    destruct(key);
    return timed_out;
}

//! Return current number of unacknowledged inflight messages.
int size() {
    Thread.MutexKey key = window_mutex->lock();
    int s = sizeof(window);
    destruct(key);
    return s;
}

//! Clear all inflight messages on session termination.
void clear() {
    Thread.MutexKey key = window_mutex->lock();
    window = ([]);
    destruct(key);
}
