// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Client Session Entity and State Container

string client_id;
int session_expiry_interval;
int created_at;
int disconnected_at;
int is_active;
object current_actor;
object inflight_window;
object offline_queue;
object packet_id_pool;
object will_message;
mapping(string:object) subscriptions;
Thread.Mutex session_mutex;

void create(string _cid, int|void _expiry, int|void _rx_max) {
    client_id = _cid;
    session_expiry_interval = zero_type(_expiry) ? Binnacle.Constants.DEFAULT_SESSION_EXPIRY : _expiry;
    created_at = time();
    disconnected_at = 0;
    is_active = 0;
    current_actor = 0;
    inflight_window = Binnacle.Engine.InflightWindow(_rx_max);
    offline_queue = .OfflineQueue();
    packet_id_pool = Binnacle.Engine.PacketIdPool();
    will_message = 0;
    subscriptions = ([]);
    session_mutex = Thread.Mutex();
}

//! Bind an active connection actor to this session.
void attach_connection(object actor) {
    Thread.MutexKey key = session_mutex->lock();
    current_actor = actor;
    is_active = 1;
    disconnected_at = 0;
    destruct(key);
}

//! Detach connection actor on network disconnect.
void detach_connection() {
    Thread.MutexKey key = session_mutex->lock();
    current_actor = 0;
    is_active = 0;
    disconnected_at = time();
    destruct(key);
}

//! Check if session has expired based on Session Expiry Interval.
int is_expired(int|void now) {
    int current_time = zero_type(now) ? time() : now;
    Thread.MutexKey key = session_mutex->lock();
    if (is_active) {
        destruct(key);
        return 0; // Active sessions never expire
    }
    if (session_expiry_interval == 0) {
        destruct(key);
        return 1; // Expire immediately on disconnect
    }
    if (session_expiry_interval == 0xFFFFFFFF) {
        destruct(key);
        return 0; // Never expires
    }
    int expired = (disconnected_at > 0 && (current_time - disconnected_at) >= session_expiry_interval);
    destruct(key);
    return expired;
}

//! Deliver or queue a message directed to this session.
void dispatch_or_queue(object pub_pkt) {
    Thread.MutexKey key = session_mutex->lock();

    if (is_active && current_actor) {
        if (!inflight_window->is_full()) {
            // Allocate packet ID if QoS > 0
            if (pub_pkt->qos > 0) {
                int pid = packet_id_pool->acquire();
                pub_pkt->packet_id = pid;
                object inflight = Binnacle.Types.InflightMessage(
                    pid, pub_pkt->qos, pub_pkt->topic, pub_pkt->payload, pub_pkt->properties
                );
                inflight_window->add(inflight);
            }
            destruct(key);
            current_actor->send_packet(pub_pkt);
            return;
        }
    }

    // Client is offline or inflight window full: queue for future delivery
    if (pub_pkt->qos > 0) {
        offline_queue->enqueue(pub_pkt);
    }
    destruct(key);
}

//! Flush offline queued messages after reconnection.
void drain_offline_queue() {
    Thread.MutexKey key = session_mutex->lock();
    if (!is_active || !current_actor) {
        destruct(key);
        return;
    }

    while (!offline_queue->is_empty() && !inflight_window->is_full()) {
        object pkt = offline_queue->dequeue();
        if (!pkt) break;

        if (pkt->qos > 0) {
            int pid = packet_id_pool->acquire();
            pkt->packet_id = pid;
            object inflight = Binnacle.Types.InflightMessage(
                pid, pkt->qos, pkt->topic, pkt->payload, pkt->properties
            );
            inflight_window->add(inflight);
        }
        current_actor->send_packet(pkt);
    }
    destruct(key);
}

//! Clear all session data on CleanStart=1.
void clear() {
    Thread.MutexKey key = session_mutex->lock();
    inflight_window->clear();
    offline_queue->clear();
    subscriptions = ([]);
    will_message = 0;
    destruct(key);
}
