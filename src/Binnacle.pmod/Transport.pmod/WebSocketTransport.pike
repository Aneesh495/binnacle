// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// MQTT over WebSocket Ingress Transport (RFC 6455)

object broker;
string bind_address;
int port;
Stdio.Port listener;
Thread.Mutex ws_mutex;

void create(object _broker, string|void _addr, int|void _port) {
    broker = _broker;
    bind_address = _addr || "0.0.0.0";
    port = zero_type(_port) ? Binnacle.Constants.DEFAULT_WEBSOCKET_PORT : _port;
    listener = 0;
    ws_mutex = Thread.Mutex();
}

//! Start the WebSocket listener port.
int start() {
    Thread.MutexKey key = ws_mutex->lock();
    listener = Stdio.Port();

    int bound = 0;
    if (bind_address == "0.0.0.0") {
        bound = listener->bind(port, _on_accept);
    } else {
        bound = listener->bind(port, _on_accept, bind_address);
    }

    if (!bound) {
        Binnacle.Utils.Logger.warn("ws", sprintf("Failed to bind WebSocket port %d", port));
        listener = 0;
        destruct(key);
        return 0;
    }

    Binnacle.Utils.Logger.info("ws", sprintf("WebSocket transport listening on %s:%d", bind_address, port));
    destruct(key);
    return 1;
}

private void _on_accept(object port_obj) {
    object sock = port_obj->accept();
    if (!sock) return;

    // WebSocket client wrapper managing handshake and frame unwrapping
    object client = WebSocketClient(sock, broker);
}

void stop() {
    Thread.MutexKey key = ws_mutex->lock();
    if (listener) {
        listener->close();
        listener = 0;
    }
    destruct(key);
}

// Inner helper class managing the lifecycle of an individual WebSocket client connection
class WebSocketClient {
    object socket;
    object broker;
    int is_upgraded;
    string buffer;
    object actor;

    void create(object _sock, object _broker) {
        socket = _sock;
        broker = _broker;
        is_upgraded = 0;
        buffer = "";
        actor = 0;

        socket->set_nonblocking(read_cb, write_cb, close_cb);
    }

    void read_cb(mixed id, string data) {
        if (!data || sizeof(data) == 0) return;
        buffer += data;

        if (!is_upgraded) {
            // Awaiting HTTP Upgrade Request
            if (search(buffer, "\r\n\r\n") >= 0 || search(buffer, "\n\n") >= 0) {
                string handshake_resp = Binnacle.Transport.WebSocketParser.generate_handshake_response(buffer);
                if (handshake_resp) {
                    socket->write(handshake_resp);
                    is_upgraded = 1;
                    buffer = ""; // Clear HTTP header

                    // Create connection actor wrapping this WebSocket connection
                    actor = Binnacle.Engine.ConnectionActor(0, broker); // Decoupled socket
                    actor->socket = this; // Route writes through this WebSocketClient
                    broker->register_actor(actor);
                } else {
                    socket->close();
                }
            }
            return;
        }

        // Unpack WebSocket binary frames
        while (1) {
            array(mixed)|int frame_res = Binnacle.Transport.WebSocketParser.decode_frame(buffer);
            if (!frame_res) break;

            object frame = frame_res[0];
            int consumed = frame_res[1];
            buffer = buffer[consumed..];

            if (frame->opcode == Binnacle.Transport.WebSocketFrame.OPCODE_BINARY) {
                if (actor) {
                    actor->read_callback(0, frame->payload);
                }
            } else if (frame->opcode == Binnacle.Transport.WebSocketFrame.OPCODE_PING) {
                // Reply with Pong
                object pong = Binnacle.Transport.WebSocketFrame(
                    Binnacle.Transport.WebSocketFrame.OPCODE_PONG, frame->payload
                );
                write(pong->encode());
            } else if (frame->opcode == Binnacle.Transport.WebSocketFrame.OPCODE_CLOSE) {
                socket->close();
                break;
            }
        }
    }

    void write_cb(mixed id) {}

    void close_cb(mixed id) {
        if (actor) {
            actor->close_callback(0);
        }
    }

    //! Intercept socket writes from actor to wrap outgoing MQTT packets in WebSocket binary frames.
    int write(string mqtt_bytes) {
        object frame = Binnacle.Transport.WebSocketFrame(
            Binnacle.Transport.WebSocketFrame.OPCODE_BINARY, mqtt_bytes
        );
        string ws_data = frame->encode();
        return socket ? socket->write(ws_data) : 0;
    }

    void close() {
        if (socket) {
            socket->close();
            socket = 0;
        }
    }

    void set_nonblocking(mixed r, mixed w, mixed c) {}
}
