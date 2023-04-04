// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Distributed Cluster Mesh Coordinator

object broker;
string node_id;
int bind_port;
Stdio.Port listener;
mapping(string:object) peer_connections; // host:port -> PeerConnection
Thread.Mutex mesh_mutex;

void create(object _broker, string _node_id, int|void _port) {
    broker = _broker;
    node_id = _node_id;
    bind_port = zero_type(_port) ? 9883 : _port;
    listener = 0;
    peer_connections = ([]);
    mesh_mutex = Thread.Mutex();
}

//! Start cluster mesh listener and connect to static peers.
int start(array(string)|void peers) {
    Thread.MutexKey key = mesh_mutex->lock();

    listener = Stdio.Port();
    if (!listener->bind(bind_port, _accept_callback)) {
        Binnacle.Utils.Logger.error("cluster", sprintf("Failed to bind cluster mesh port %d", bind_port));
        listener = 0;
        destruct(key);
        return 0;
    }

    Binnacle.Utils.Logger.info("cluster", sprintf("Cluster mesh listening on port %d (node: %s)", bind_port, node_id));

    if (peers && sizeof(peers)) {
        foreach (peers, string p_addr) {
            array(string) parts = p_addr / ":";
            if (sizeof(parts) == 2) {
                string host = parts[0];
                int port = (int)parts[1];
                string key_str = sprintf("%s:%d", host, port);
                object peer = .PeerConnection(host, port, node_id, this);
                peer_connections[key_str] = peer;
                peer->connect();
            }
        }
    }

    destruct(key);
    return 1;
}

private void _accept_callback(mixed|void id) {
    if (!listener) return;
    object sock = listener->accept();
    if (!sock) return;

    // Incoming cluster peer connection
    Binnacle.Utils.Logger.info("cluster", "Accepted incoming cluster peer connection");
    // Handled via listener peer
}

//! Broadcast a locally published message to all active cluster peers.
void broadcast_publish(string topic, string payload, int qos, int retain) {
    Thread.MutexKey key = mesh_mutex->lock();
    if (sizeof(peer_connections) == 0) {
        destruct(key);
        return;
    }

    array(object) peers_snapshot = values(peer_connections);
    destruct(key);

    foreach (peers_snapshot, object peer) {
        peer->forward_publish(node_id, 0, topic, payload, qos, retain);
    }
}

//! Invoked when a replicated message arrives from a remote peer.
void on_replicated_publish(string origin_id, int hops, string topic, string payload, int qos, int retain) {
    // Inject into local broker dispatch
    if (broker) {
        broker->internal_publish(topic, payload, qos, retain);
    }

    // Gossip forward to other peers (except origin) if hops allow
    Thread.MutexKey key = mesh_mutex->lock();
    array(object) peers_snapshot = values(peer_connections);
    destruct(key);

    foreach (peers_snapshot, object peer) {
        peer->forward_publish(origin_id, hops, topic, payload, qos, retain);
    }
}

//! Stop cluster mesh and terminate peer connections.
void stop() {
    Thread.MutexKey key = mesh_mutex->lock();
    if (listener) {
        listener->close();
        listener = 0;
    }
    foreach (values(peer_connections), object peer) {
        peer->close();
    }
    peer_connections = ([]);
    destruct(key);
}
