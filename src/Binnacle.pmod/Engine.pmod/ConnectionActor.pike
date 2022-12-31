// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Client Connection Actor and Transport State Machine

object socket;
object broker;
string client_id;
int protocol_version;
int is_connected;
int keep_alive;
int last_active_time;
string read_buffer;
object qos_handler;
object topic_alias_map;
object rate_limiter;
object session;
Thread.Mutex send_mutex;

void create(object _sock, object _broker) {
    socket = _sock;
    broker = _broker;
    client_id = "";
    protocol_version = Binnacle.Constants.PROTOCOL_LEVEL_5_0;
    is_connected = 0;
    keep_alive = 60;
    last_active_time = time();
    read_buffer = "";
    qos_handler = .QoSHandler();
    topic_alias_map = Binnacle.Router.TopicAliasMap();
    rate_limiter = .RateLimiter(0); // Unlimited by default
    session = 0;
    send_mutex = Thread.Mutex();

    if (socket) {
        socket->set_nonblocking(read_callback, write_callback, close_callback);
    }
}

//! Non-blocking socket read callback.
void read_callback(mixed id, string data) {
    if (!data || sizeof(data) == 0) return;

    last_active_time = time();
    broker->record_bytes_received(sizeof(data));

    if (!rate_limiter->consume(sizeof(data))) {
        Binnacle.Utils.Logger.warn("actor", sprintf("Client %s rate limited, dropping connection", client_id));
        disconnect(Binnacle.Constants.RC_MESSAGE_RATE_TOO_HIGH, "Rate limit exceeded");
        return;
    }

    read_buffer += data;

    // Drain complete packets from buffer
    while (1) {
        array(mixed)|int parse_res = 0;
        catch {
            parse_res = Binnacle.Protocol.PacketParser.parse_packet(read_buffer, protocol_version);
        };

        if (!parse_res) {
            break; // Awaiting more bytes
        }

        object pkt = parse_res[0];
        int consumed = parse_res[1];
        read_buffer = read_buffer[consumed..];

        broker->record_message_received();
        _handle_packet(pkt);
    }
}

//! Socket write callback.
void write_callback(mixed id) {
    // Socket ready for more writes
}

//! Socket close callback.
void close_callback(mixed id) {
    if (is_connected) {
        is_connected = 0;
        broker->handle_client_disconnected(this, 1); // Unexpected drop, fire Will
    }
}

//! Internal packet dispatcher.
private void _handle_packet(object pkt) {
    int ptype = pkt->packet_type;

    if (!is_connected && ptype != Binnacle.Constants.PACKET_CONNECT) {
        // First packet MUST be CONNECT
        disconnect(Binnacle.Constants.RC_PROTOCOL_ERROR, "First packet must be CONNECT");
        return;
    }

    switch (ptype) {
        case Binnacle.Constants.PACKET_CONNECT:
            broker->handle_connect(this, pkt);
            break;

        case Binnacle.Constants.PACKET_PUBLISH:
            broker->handle_publish(this, pkt);
            break;

        case Binnacle.Constants.PACKET_PUBACK:
            if (session) {
                qos_handler->handle_outbound_puback(pkt, session->inflight_window, session->packet_id_pool);
            }
            break;

        case Binnacle.Constants.PACKET_PUBREC:
            if (session) {
                object rel = qos_handler->handle_outbound_pubrec(pkt, session->inflight_window);
                if (rel) send_packet(rel);
            }
            break;

        case Binnacle.Constants.PACKET_PUBREL: {
            object comp = qos_handler->handle_inbound_pubrel(pkt, protocol_version);
            send_packet(comp);
            break;
        }

        case Binnacle.Constants.PACKET_PUBCOMP:
            if (session) {
                qos_handler->handle_outbound_pubcomp(pkt, session->inflight_window, session->packet_id_pool);
            }
            break;

        case Binnacle.Constants.PACKET_SUBSCRIBE:
            broker->handle_subscribe(this, pkt);
            break;

        case Binnacle.Constants.PACKET_UNSUBSCRIBE:
            broker->handle_unsubscribe(this, pkt);
            break;

        case Binnacle.Constants.PACKET_PINGREQ:
            send_packet(Binnacle.Protocol.PingRespPacket());
            break;

        case Binnacle.Constants.PACKET_DISCONNECT:
            is_connected = 0;
            broker->handle_client_disconnected(this, 0); // Clean disconnect, do NOT fire Will
            close_socket();
            break;

        default:
            break;
    }
}

//! Transmit a typed packet to the client.
int send_packet(object pkt) {
    if (!socket) return 0;

    string wire_bytes = "";
    catch {
        wire_bytes = pkt->encode(protocol_version);
    };

    if (sizeof(wire_bytes) == 0) return 0;

    Thread.MutexKey key = send_mutex->lock();
    int written = 0;
    catch {
        written = socket->write(wire_bytes);
    };
    destruct(key);

    if (written > 0) {
        broker->record_bytes_sent(written);
        broker->record_message_sent();
    }
    return written == sizeof(wire_bytes);
}

//! Force disconnect with MQTT 5.0 reason code and close socket.
void disconnect(int|void reason_code, string|void reason_string) {
    if (is_connected && protocol_version >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
        int rc = zero_type(reason_code) ? Binnacle.Constants.RC_NORMAL_DISCONNECTION : reason_code;
        mapping(int:mixed) props = ([]);
        if (reason_string && sizeof(reason_string)) {
            props[Binnacle.Constants.PROP_REASON_STRING] = reason_string;
        }
        send_packet(Binnacle.Protocol.DisconnectPacket(rc, props));
    }
    is_connected = 0;
    close_socket();
}

//! Safely terminate the underlying socket.
void close_socket() {
    if (socket) {
        catch {
            socket->set_nonblocking(0, 0, 0);
            socket->close();
        };
        socket = 0;
    }
}

//! Verify if keep-alive watchdog timer has expired.
void check_keep_alive(int now) {
    if (keep_alive <= 0 || !is_connected) return;
    int max_idle = (int)(keep_alive * 1.5);
    if ((now - last_active_time) > max_idle) {
        Binnacle.Utils.Logger.warn("actor", sprintf("Keep alive timeout expired for client %s", client_id));
        disconnect(Binnacle.Constants.RC_KEEP_ALIVE_TIMEOUT, "Keep-alive ping timeout");
    }
}
