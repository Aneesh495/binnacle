#!/usr/bin/env pike
// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Native CLI Subscriber Client

int main(int argc, array(string) argv) {
    string host = "127.0.0.1";
    int port = 1883;
    string filter = "#";
    int qos = 0;
    string client_id = sprintf("binnacle-sub-%d", time());

    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if ((arg == "-h" || arg == "--host") && (i + 1) < argc) {
            host = argv[++i];
        } else if ((arg == "-p" || arg == "--port") && (i + 1) < argc) {
            sscanf(argv[++i], "%d", port);
        } else if ((arg == "-t" || arg == "--topic") && (i + 1) < argc) {
            filter = argv[++i];
        } else if ((arg == "-q" || arg == "--qos") && (i + 1) < argc) {
            sscanf(argv[++i], "%d", qos);
        } else if (arg == "--client-id" && (i + 1) < argc) {
            client_id = argv[++i];
        } else if (arg == "--help") {
            write("Binnacle CLI Subscriber\n");
            write("Usage: pike -Msrc src/bin/binnacle_sub.pike [OPTIONS]\n\n");
            write("Options:\n");
            write("  -h, --host <host>       Broker host (default: 127.0.0.1)\n");
            write("  -p, --port <port>       Broker port (default: 1883)\n");
            write("  -t, --topic <filter>    Topic filter pattern (default: #)\n");
            write("  -q, --qos <0|1|2>       Requested Quality of Service level (default: 0)\n");
            write("  --client-id <id>        Client identifier\n");
            return 0;
        }
    }

    Stdio.File sock = Stdio.File();
    if (!sock->open_socket()) {
        werror("Error: Failed to open socket\n");
        return 1;
    }

    if (!sock->connect(host, port)) {
        werror("Error: Failed to connect to %s:%d\n", host, port);
        return 1;
    }

    // 1. Send CONNECT
    object cp = Binnacle.Protocol.ConnectPacket(client_id, 5, 1, 60);
    sock->write(cp->encode());

    // 2. Read CONNACK
    string resp = sock->read(256, 1);
    if (!resp || sizeof(resp) < 2) {
        werror("Error: Failed to receive CONNACK\n");
        sock->close();
        return 1;
    }

    // 3. Send SUBSCRIBE
    object sub = Binnacle.Protocol.SubscribePacket(1);
    sub->add_subscription(filter, qos);
    sock->write(sub->encode());

    // 4. Read SUBACK
    string suback_bytes = sock->read(256, 1);
    write("Subscribed to %s with requested QoS %d. Listening for telemetry...\n", filter, qos);

    // 5. Streaming read loop
    string stream_buffer = "";
    while (1) {
        string chunk = sock->read(4096, 1);
        if (!chunk || sizeof(chunk) == 0) break;
        stream_buffer += chunk;

        while (1) {
            array(mixed)|int parse_res = 0;
            catch {
                parse_res = Binnacle.Protocol.PacketParser.parse_packet(stream_buffer, 5);
            };

            if (!parse_res) break;

            object pkt = parse_res[0];
            int consumed = parse_res[1];
            stream_buffer = stream_buffer[consumed..];

            if (pkt->packet_type == Binnacle.Constants.PACKET_PUBLISH) {
                write("[%s] topic: %s (QoS %d, Retain %d) -> %s\n",
                      Binnacle.Utils.Time.format_iso8601(),
                      pkt->topic, pkt->qos, pkt->retain, pkt->payload);

                // Acknowledge QoS 1/2 if needed
                if (pkt->qos == 1) {
                    object ack = Binnacle.Protocol.PubAckPacket(pkt->packet_id, Binnacle.Constants.RC_SUCCESS);
                    sock->write(ack->encode());
                } else if (pkt->qos == 2) {
                    object rec = Binnacle.Protocol.PubRecPacket(pkt->packet_id, Binnacle.Constants.RC_SUCCESS);
                    sock->write(rec->encode());
                }
            } else if (pkt->packet_type == Binnacle.Constants.PACKET_PUBREL) {
                object comp = Binnacle.Protocol.PubCompPacket(pkt->packet_id, Binnacle.Constants.RC_SUCCESS);
                sock->write(comp->encode());
            }
        }
    }

    sock->close();
    return 0;
}
