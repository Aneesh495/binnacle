// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Asynchronous Non-Blocking TCP Ingress Server

object broker;
string bind_address;
int port;
int backlog;
Stdio.Port listener;
Thread.Mutex server_mutex;

void create(object _broker, string|void _addr, int|void _port, int|void _backlog) {
    broker = _broker;
    bind_address = _addr || "0.0.0.0";
    port = zero_type(_port) ? Binnacle.Constants.DEFAULT_PORT : _port;
    backlog = zero_type(_backlog) ? 1024 : _backlog;
    listener = 0;
    server_mutex = Thread.Mutex();
}

//! Start the TCP listener server.
int start() {
    Thread.MutexKey key = server_mutex->lock();

    listener = Stdio.Port();
    // In Pike: bind(port, accept_callback, host)
    int bound = 0;
    if (bind_address == "0.0.0.0") {
        bound = listener->bind(port, _on_accept);
    } else {
        bound = listener->bind(port, _on_accept, bind_address);
    }

    if (!bound) {
        Binnacle.Utils.Logger.fatal("tcp", sprintf("Failed to bind TCP listener on %s:%d", bind_address, port));
        listener = 0;
        destruct(key);
        return 0;
    }

    Binnacle.Utils.Logger.info("tcp", sprintf("TCP listener active on %s:%d", bind_address, port));
    destruct(key);
    return 1;
}

//! Accept callback invoked by Pike backend on client socket connection.
private void _on_accept(mixed|void id) {
    if (!listener) return;
    object client_sock = listener->accept();
    if (!client_sock) return;

    // Instantiate client connection actor
    object actor = Binnacle.Engine.ConnectionActor(client_sock, broker);
    broker->register_actor(actor);
}

//! Stop the TCP server.
void stop() {
    Thread.MutexKey key = server_mutex->lock();
    if (listener) {
        listener->close();
        listener = 0;
    }
    destruct(key);
    Binnacle.Utils.Logger.info("tcp", "TCP listener stopped");
}
