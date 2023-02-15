// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Cluster Peer Connection and Loop Prevention Actor

constant MAX_CLUSTER_HOPS = 8;

string peer_host;
int peer_port;
string peer_node_id;
string local_node_id;
object socket;
object mesh_ref;
int is_connected;
int last_heartbeat_time;
string read_buffer;
mixed reconnect_callout;
Thread.Mutex peer_mutex;

void create(string _host, int _port, string _local_id, object _mesh) {
    peer_host = _host;
    peer_port = _port;
    local_node_id = _local_id;
    mesh_ref = _mesh;
    peer_node_id = "";
    socket = 0;
    is_connected = 0;
    last_heartbeat_time = time();
    read_buffer = "";
    reconnect_callout = 0;
    peer_mutex = Thread.Mutex();
}

//! Initiate connection to remote cluster peer.
void connect() {
    Thread.MutexKey key = peer_mutex->lock();
    socket = Stdio.File();
    if (!socket->open_socket()) {
        destruct(key);
        _schedule_reconnect();
        return;
    }

    int res = socket->connect(peer_host, peer_port);
    if (!res) {
        socket->close();
        socket = 0;
        destruct(key);
        _schedule_reconnect();
        return;
    }

    is_connected = 1;
    socket->set_nonblocking(read_callback, write_callback, close_callback);

    // Transmit HELLO
    string hello_frame = .ReplicationProtocol.encode_frame(
        .ReplicationProtocol.MSG_HELLO, local_node_id, 0, local_node_id
    );
    socket->write(hello_frame);
    destruct(key);

    Binnacle.Utils.Logger.info("cluster", sprintf("Connected to cluster peer at %s:%d", peer_host, peer_port));
}

void read_callback(mixed id, string data) {
    if (!data || sizeof(data) == 0) return;
    read_buffer += data;

    while (1) {
        array(mixed)|int frame_res = 0;
        catch {
            frame_res = .ReplicationProtocol.decode_frame(read_buffer);
        };

        if (!frame_res) break;

        mapping frame = frame_res[0];
        int consumed = frame_res[1];
        read_buffer = read_buffer[consumed..];

        _handle_frame(frame);
    }
}

void write_callback(mixed id) {}

void close_callback(mixed id) {
    Thread.MutexKey key = peer_mutex->lock();
    is_connected = 0;
    if (socket) {
        socket->close();
        socket = 0;
    }
    destruct(key);

    Binnacle.Utils.Logger.warn("cluster", sprintf("Lost connection to cluster peer %s:%d", peer_host, peer_port));
    _schedule_reconnect();
}

private void _schedule_reconnect() {
    if (!reconnect_callout) {
        reconnect_callout = call_out(_reconnect_tick, 5);
    }
}

private void _reconnect_tick() {
    reconnect_callout = 0;
    connect();
}

//! Forward a message to this cluster peer.
//! Enforces loop prevention using hop counter and origin node check.
int forward_publish(string origin_id, int hops, string topic, string payload, int qos, int retain) {
    // Loop prevention checks
    if (origin_id == peer_node_id) {
        return 0; // Do not reflect message back to sender
    }
    if (hops >= MAX_CLUSTER_HOPS) {
        Binnacle.Utils.Logger.warn("cluster", sprintf("Dropping message on %s: hop limit %d exceeded", topic, hops));
        return 0;
    }

    Thread.MutexKey key = peer_mutex->lock();
    if (!is_connected || !socket) {
        destruct(key);
        return 0;
    }

    mapping msg_data = ([
        "topic": topic,
        "payload": payload,
        "qos": qos,
        "retain": retain
    ]);
    string payload_json = Standards.JSON.encode(msg_data);

    string frame = .ReplicationProtocol.encode_frame(
        .ReplicationProtocol.MSG_FORWARD_PUBLISH, origin_id, hops + 1, payload_json
    );

    int written = 0;
    catch {
        written = socket->write(frame);
    };
    destruct(key);

    return written == sizeof(frame);
}

private void _handle_frame(mapping frame) {
    int f_type = frame->type;

    if (f_type == .ReplicationProtocol.MSG_HELLO) {
        peer_node_id = frame->origin;
        Binnacle.Utils.Logger.info("cluster", sprintf("Cluster peer identified as node %s", peer_node_id));
    } else if (f_type == .ReplicationProtocol.MSG_FORWARD_PUBLISH) {
        string origin = frame->origin;
        int hops = frame->hops;

        if (origin == local_node_id) {
            return; // Loop drop: originated from this node
        }

        mixed parsed = 0;
        catch {
            parsed = Standards.JSON.decode(frame->payload);
        };

        if (parsed && mesh_ref) {
            mesh_ref->on_replicated_publish(origin, hops, parsed->topic, parsed->payload, parsed->qos, parsed->retain);
        }
    }
}

void close() {
    Thread.MutexKey key = peer_mutex->lock();
    if (reconnect_callout) {
        remove_call_out(reconnect_callout);
        reconnect_callout = 0;
    }
    if (socket) {
        socket->close();
        socket = 0;
    }
    is_connected = 0;
    destruct(key);
}
