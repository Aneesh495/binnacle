#!/usr/bin/env pike
// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Native CLI Publisher Client

int main(int argc, array(string) argv) {
    string host = "127.0.0.1";
    int port = 1883;
    string topic = "";
    string message = "";
    int qos = 0;
    int retain = 0;
    string client_id = sprintf("binnacle-pub-%d", time());

    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if ((arg == "-h" || arg == "--host") && (i + 1) < argc) {
            host = argv[++i];
        } else if ((arg == "-p" || arg == "--port") && (i + 1) < argc) {
            sscanf(argv[++i], "%d", port);
        } else if ((arg == "-t" || arg == "--topic") && (i + 1) < argc) {
            topic = argv[++i];
        } else if ((arg == "-m" || arg == "--message") && (i + 1) < argc) {
            message = argv[++i];
        } else if ((arg == "-q" || arg == "--qos") && (i + 1) < argc) {
            sscanf(argv[++i], "%d", qos);
        } else if (arg == "-r" || arg == "--retain") {
            retain = 1;
        } else if (arg == "--client-id" && (i + 1) < argc) {
            client_id = argv[++i];
        } else if (arg == "--help") {
            write("Binnacle CLI Publisher\n");
            write("Usage: pike -Msrc src/bin/binnacle_pub.pike -t <topic> -m <message> [OPTIONS]\n\n");
            write("Options:\n");
            write("  -h, --host <host>       Broker host (default: 127.0.0.1)\n");
            write("  -p, --port <port>       Broker port (default: 1883)\n");
            write("  -t, --topic <topic>     Target publish topic (required)\n");
            write("  -m, --message <msg>     Message payload string (required)\n");
            write("  -q, --qos <0|1|2>       Quality of Service level (default: 0)\n");
            write("  -r, --retain            Set RETAIN flag\n");
            write("  --client-id <id>        Client identifier\n");
            return 0;
        }
    }

    if (sizeof(topic) == 0) {
        werror("Error: Topic is required (-t <topic>)\n");
        return 1;
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

    array(mixed) ca_res = Binnacle.Protocol.PacketParser.parse_packet(resp, 5);
    if (!ca_res || ca_res[0]->packet_type != Binnacle.Constants.PACKET_CONNACK) {
        werror("Error: Invalid CONNACK received\n");
        sock->close();
        return 1;
    }

    // 3. Send PUBLISH
    int pid = (qos > 0) ? 1 : 0;
    object pub = Binnacle.Protocol.PublishPacket(topic, message, qos, retain, 0, pid);
    sock->write(pub->encode());

    // 4. Handle QoS acknowledgment
    if (qos == 1) {
        string ack_bytes = sock->read(256, 1);
        array(mixed) ack_res = Binnacle.Protocol.PacketParser.parse_packet(ack_bytes, 5);
        if (ack_res && ack_res[0]->packet_type == Binnacle.Constants.PACKET_PUBACK) {
            write("Published (QoS 1, PUBACK received, pid=%d)\n", ack_res[0]->packet_id);
        }
    } else if (qos == 2) {
        string rec_bytes = sock->read(256, 1);
        array(mixed) rec_res = Binnacle.Protocol.PacketParser.parse_packet(rec_bytes, 5);
        if (rec_res && rec_res[0]->packet_type == Binnacle.Constants.PACKET_PUBREC) {
            // Send PUBREL
            object rel = Binnacle.Protocol.PubRelPacket(pid, Binnacle.Constants.RC_SUCCESS);
            sock->write(rel->encode());

            // Await PUBCOMP
            string comp_bytes = sock->read(256, 1);
            array(mixed) comp_res = Binnacle.Protocol.PacketParser.parse_packet(comp_bytes, 5);
            if (comp_res && comp_res[0]->packet_type == Binnacle.Constants.PACKET_PUBCOMP) {
                write("Published (QoS 2, 4-step handshake complete, pid=%d)\n", pid);
            }
        }
    } else {
        write("Published (QoS 0 fire-and-forget)\n");
    }

    // 5. Send DISCONNECT
    object disc = Binnacle.Protocol.DisconnectPacket();
    sock->write(disc->encode());
    sock->close();
    return 0;
}
