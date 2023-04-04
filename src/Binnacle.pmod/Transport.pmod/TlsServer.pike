// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// TLS Encrypted Socket Ingress Server

object broker;
string bind_address;
int port;
string cert_path;
string key_path;
object ssl_port;
Thread.Mutex tls_mutex;

void create(object _broker, string|void _addr, int|void _port,
            string|void _cert, string|void _key) {
    broker = _broker;
    bind_address = _addr || "0.0.0.0";
    port = zero_type(_port) ? Binnacle.Constants.DEFAULT_TLS_PORT : _port;
    cert_path = _cert || "";
    key_path = _key || "";
    ssl_port = 0;
    tls_mutex = Thread.Mutex();
}

//! Start TLS server if valid certificates are provided.
int start() {
    Thread.MutexKey key = tls_mutex->lock();

    if (!sizeof(cert_path) || !sizeof(key_path) || !Stdio.exist(cert_path) || !Stdio.exist(key_path)) {
        Binnacle.Utils.Logger.info("tls", "TLS certificates not configured, TLS ingress disabled");
        destruct(key);
        return 0;
    }

    catch {
        // Pike SSL.Port initialization
        object ctx = SSL.Context();
        string cert_pem = Stdio.read_file(cert_path);
        string key_pem = Stdio.read_file(key_path);
        ctx->certificates = ({ ({ cert_pem, key_pem }) });

        ssl_port = SSL.Port(ctx);
        int bound = ssl_port->bind(port, _on_accept, bind_address);
        if (bound) {
            Binnacle.Utils.Logger.info("tls", sprintf("TLS ingress active on %s:%d", bind_address, port));
            destruct(key);
            return 1;
        }
    };

    Binnacle.Utils.Logger.warn("tls", sprintf("Failed to bind TLS port %d", port));
    ssl_port = 0;
    destruct(key);
    return 0;
}

private void _on_accept(mixed|void id) {
    if (!ssl_port) return;
    object client_sock = ssl_port->accept();
    if (!client_sock) return;

    object actor = Binnacle.Engine.ConnectionActor(client_sock, broker);
    broker->register_actor(actor);
}

void stop() {
    Thread.MutexKey key = tls_mutex->lock();
    if (ssl_port) {
        ssl_port->close();
        ssl_port = 0;
    }
    destruct(key);
}
