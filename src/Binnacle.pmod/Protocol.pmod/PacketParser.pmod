// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Wire Protocol Streaming Deserializer and Packet Parser

//! Parse a single MQTT packet from raw binary stream buffer.
//! Returns ({ Packet pkt, int bytes_consumed }) if a full packet was parsed.
//! Returns 0 if buffer does not yet contain a complete packet (needs more data).
//! Throws MalformedPacketError or ProtocolError on invalid protocol wire framing.
array(mixed)|int parse_packet(string buf, int|void active_proto_version) {
    int buf_len = sizeof(buf);
    if (buf_len < 2) return 0; // Fixed header requires at least 2 bytes

    int first_byte = buf[0];
    int packet_type = (first_byte >> 4) & 0x0F;
    int flags = first_byte & 0x0F;

    if (packet_type < 1 || packet_type > 15) {
        throw(Binnacle.Errors.MalformedPacketError(
            sprintf("Invalid MQTT packet type in fixed header: %d", packet_type),
            Binnacle.Constants.RC_MALFORMED_PACKET
        ));
    }

    // Attempt to decode variable-byte Remaining Length
    int remaining_length;
    int varint_bytes;
    catch {
        array(int) vi_res = Binnacle.Protocol.VarInt.decode(buf, 1);
        remaining_length = vi_res[0];
        varint_bytes = vi_res[1];
    };

    if (varint_bytes == 0) {
        // VarInt was truncated, await more network bytes
        return 0;
    }

    int header_len = 1 + varint_bytes;
    int total_len = header_len + remaining_length;

    if (buf_len < total_len) {
        // Full packet payload not yet arrived
        return 0;
    }

    // Validate fixed header flags
    object hdr = Binnacle.Protocol.PacketHeader(packet_type, flags, remaining_length, header_len);
    if (!hdr->validate_flags()) {
        throw(Binnacle.Errors.MalformedPacketError(
            sprintf("Invalid fixed header flags 0x%x for packet type %s",
                    flags, Binnacle.Constants.packet_type_name(packet_type)),
            Binnacle.Constants.RC_MALFORMED_PACKET
        ));
    }

    string body = buf[header_len .. total_len - 1];
    int idx = 0;
    int ver = zero_type(active_proto_version) ? Binnacle.Constants.PROTOCOL_LEVEL_5_0 : active_proto_version;
    object pkt = 0;

    switch (packet_type) {
        case Binnacle.Constants.PACKET_CONNECT: {
            // Protocol Name
            int name_len;
            sscanf(body[idx..idx+1], "%2c", name_len);
            idx += 2;
            string proto_name = body[idx..idx+name_len-1];
            idx += name_len;

            if (proto_name != Binnacle.Constants.PROTOCOL_NAME_MQTT) {
                throw(Binnacle.Errors.ProtocolError(
                    sprintf("Unsupported protocol name: %s", proto_name),
                    Binnacle.Constants.RC_UNSUPPORTED_PROTOCOL_VERSION
                ));
            }

            int proto_ver = body[idx++];
            ver = proto_ver;

            int conn_flags = body[idx++];
            int clean_start = (conn_flags & Binnacle.Constants.CONNECT_FLAG_CLEAN_START) ? 1 : 0;
            int will_flag = (conn_flags & Binnacle.Constants.CONNECT_FLAG_WILL) ? 1 : 0;
            int will_qos = (conn_flags >> 3) & 0x03;
            int will_retain = (conn_flags & Binnacle.Constants.CONNECT_FLAG_WILL_RETAIN) ? 1 : 0;
            int password_flag = (conn_flags & Binnacle.Constants.CONNECT_FLAG_PASSWORD) ? 1 : 0;
            int username_flag = (conn_flags & Binnacle.Constants.CONNECT_FLAG_USERNAME) ? 1 : 0;

            int keep_alive;
            sscanf(body[idx..idx+1], "%2c", keep_alive);
            idx += 2;

            mapping(int:mixed) connect_props = ([]);
            if (proto_ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
                array(mixed) prop_res = Binnacle.Protocol.Properties.decode_properties(body, idx);
                connect_props = prop_res[0];
                idx += prop_res[1];
            }

            // Client ID
            int cid_len;
            sscanf(body[idx..idx+1], "%2c", cid_len);
            idx += 2;
            string cid = (cid_len > 0) ? body[idx..idx+cid_len-1] : "";
            idx += cid_len;

            object cp = Binnacle.Protocol.ConnectPacket(cid, proto_ver, clean_start, keep_alive);
            cp->properties = connect_props;

            // Will
            if (will_flag) {
                mapping(int:mixed) will_props = ([]);
                if (proto_ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
                    array(mixed) wp_res = Binnacle.Protocol.Properties.decode_properties(body, idx);
                    will_props = wp_res[0];
                    idx += wp_res[1];
                }

                int wt_len;
                sscanf(body[idx..idx+1], "%2c", wt_len);
                idx += 2;
                string wt = body[idx..idx+wt_len-1];
                idx += wt_len;

                int wp_len;
                sscanf(body[idx..idx+1], "%2c", wp_len);
                idx += 2;
                string wp_payload = body[idx..idx+wp_len-1];
                idx += wp_len;

                cp->set_will(wt, wp_payload, will_qos, will_retain, will_props);
            }

            // Username
            if (username_flag) {
                int u_len;
                sscanf(body[idx..idx+1], "%2c", u_len);
                idx += 2;
                cp->username = body[idx..idx+u_len-1];
                cp->username_flag = 1;
                idx += u_len;
            }

            // Password
            if (password_flag) {
                int p_len;
                sscanf(body[idx..idx+1], "%2c", p_len);
                idx += 2;
                cp->password = body[idx..idx+p_len-1];
                cp->password_flag = 1;
                idx += p_len;
            }

            pkt = cp;
            break;
        }

        case Binnacle.Constants.PACKET_CONNACK: {
            int sp = (body[idx++] & 0x01);
            int rc = body[idx++];
            mapping(int:mixed) props = ([]);
            if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0 && idx < sizeof(body)) {
                array(mixed) prop_res = Binnacle.Protocol.Properties.decode_properties(body, idx);
                props = prop_res[0];
                idx += prop_res[1];
            }
            pkt = Binnacle.Protocol.ConnAckPacket(sp, rc, props);
            break;
        }

        case Binnacle.Constants.PACKET_PUBLISH: {
            int qos = hdr->qos;
            int dup = hdr->dup;
            int retain = hdr->retain;

            int top_len;
            sscanf(body[idx..idx+1], "%2c", top_len);
            idx += 2;
            string topic = (top_len > 0) ? body[idx..idx+top_len-1] : "";
            idx += top_len;

            int packet_id = 0;
            if (qos > 0) {
                sscanf(body[idx..idx+1], "%2c", packet_id);
                idx += 2;
            }

            mapping(int:mixed) pub_props = ([]);
            if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
                array(mixed) prop_res = Binnacle.Protocol.Properties.decode_properties(body, idx);
                pub_props = prop_res[0];
                idx += prop_res[1];
            }

            string payload = (idx < sizeof(body)) ? body[idx..] : "";
            pkt = Binnacle.Protocol.PublishPacket(topic, payload, qos, retain, dup, packet_id, pub_props);
            break;
        }

        case Binnacle.Constants.PACKET_PUBACK: {
            int pid;
            sscanf(body[idx..idx+1], "%2c", pid);
            idx += 2;
            int rc = Binnacle.Constants.RC_SUCCESS;
            mapping(int:mixed) props = ([]);
            if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0 && idx < sizeof(body)) {
                rc = body[idx++];
                if (idx < sizeof(body)) {
                    array(mixed) prop_res = Binnacle.Protocol.Properties.decode_properties(body, idx);
                    props = prop_res[0];
                }
            }
            pkt = Binnacle.Protocol.PubAckPacket(pid, rc, props);
            break;
        }

        case Binnacle.Constants.PACKET_PUBREC: {
            int pid;
            sscanf(body[idx..idx+1], "%2c", pid);
            idx += 2;
            int rc = Binnacle.Constants.RC_SUCCESS;
            mapping(int:mixed) props = ([]);
            if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0 && idx < sizeof(body)) {
                rc = body[idx++];
                if (idx < sizeof(body)) {
                    array(mixed) prop_res = Binnacle.Protocol.Properties.decode_properties(body, idx);
                    props = prop_res[0];
                }
            }
            pkt = Binnacle.Protocol.PubRecPacket(pid, rc, props);
            break;
        }

        case Binnacle.Constants.PACKET_PUBREL: {
            int pid;
            sscanf(body[idx..idx+1], "%2c", pid);
            idx += 2;
            int rc = Binnacle.Constants.RC_SUCCESS;
            mapping(int:mixed) props = ([]);
            if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0 && idx < sizeof(body)) {
                rc = body[idx++];
                if (idx < sizeof(body)) {
                    array(mixed) prop_res = Binnacle.Protocol.Properties.decode_properties(body, idx);
                    props = prop_res[0];
                }
            }
            pkt = Binnacle.Protocol.PubRelPacket(pid, rc, props);
            break;
        }

        case Binnacle.Constants.PACKET_PUBCOMP: {
            int pid;
            sscanf(body[idx..idx+1], "%2c", pid);
            idx += 2;
            int rc = Binnacle.Constants.RC_SUCCESS;
            mapping(int:mixed) props = ([]);
            if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0 && idx < sizeof(body)) {
                rc = body[idx++];
                if (idx < sizeof(body)) {
                    array(mixed) prop_res = Binnacle.Protocol.Properties.decode_properties(body, idx);
                    props = prop_res[0];
                }
            }
            pkt = Binnacle.Protocol.PubCompPacket(pid, rc, props);
            break;
        }

        case Binnacle.Constants.PACKET_SUBSCRIBE: {
            int pid;
            sscanf(body[idx..idx+1], "%2c", pid);
            idx += 2;

            mapping(int:mixed) sub_props = ([]);
            if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
                array(mixed) prop_res = Binnacle.Protocol.Properties.decode_properties(body, idx);
                sub_props = prop_res[0];
                idx += prop_res[1];
            }

            array(array) subs = ({});
            while (idx < sizeof(body)) {
                int f_len;
                sscanf(body[idx..idx+1], "%2c", f_len);
                idx += 2;
                string filter = body[idx..idx+f_len-1];
                idx += f_len;
                int opt_byte = body[idx++];
                subs += ({ ({ filter, opt_byte }) });
            }

            pkt = Binnacle.Protocol.SubscribePacket(pid, subs, sub_props);
            break;
        }

        case Binnacle.Constants.PACKET_SUBACK: {
            int pid;
            sscanf(body[idx..idx+1], "%2c", pid);
            idx += 2;

            mapping(int:mixed) suback_props = ([]);
            if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
                array(mixed) prop_res = Binnacle.Protocol.Properties.decode_properties(body, idx);
                suback_props = prop_res[0];
                idx += prop_res[1];
            }

            array(int) rcs = ({});
            while (idx < sizeof(body)) {
                rcs += ({ body[idx++] });
            }

            pkt = Binnacle.Protocol.SubAckPacket(pid, rcs, suback_props);
            break;
        }

        case Binnacle.Constants.PACKET_UNSUBSCRIBE: {
            int pid;
            sscanf(body[idx..idx+1], "%2c", pid);
            idx += 2;

            mapping(int:mixed) unsub_props = ([]);
            if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
                array(mixed) prop_res = Binnacle.Protocol.Properties.decode_properties(body, idx);
                unsub_props = prop_res[0];
                idx += prop_res[1];
            }

            array(string) filters = ({});
            while (idx < sizeof(body)) {
                int f_len;
                sscanf(body[idx..idx+1], "%2c", f_len);
                idx += 2;
                filters += ({ body[idx..idx+f_len-1] });
                idx += f_len;
            }

            pkt = Binnacle.Protocol.UnsubscribePacket(pid, filters, unsub_props);
            break;
        }

        case Binnacle.Constants.PACKET_UNSUBACK: {
            int pid;
            sscanf(body[idx..idx+1], "%2c", pid);
            idx += 2;

            mapping(int:mixed) unsuback_props = ([]);
            if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0) {
                array(mixed) prop_res = Binnacle.Protocol.Properties.decode_properties(body, idx);
                unsuback_props = prop_res[0];
                idx += prop_res[1];
            }

            array(int) rcs = ({});
            while (idx < sizeof(body)) {
                rcs += ({ body[idx++] });
            }

            pkt = Binnacle.Protocol.UnsubAckPacket(pid, rcs, unsuback_props);
            break;
        }

        case Binnacle.Constants.PACKET_PINGREQ: {
            pkt = Binnacle.Protocol.PingReqPacket();
            break;
        }

        case Binnacle.Constants.PACKET_PINGRESP: {
            pkt = Binnacle.Protocol.PingRespPacket();
            break;
        }

        case Binnacle.Constants.PACKET_DISCONNECT: {
            int rc = Binnacle.Constants.RC_NORMAL_DISCONNECTION;
            mapping(int:mixed) disc_props = ([]);
            if (ver >= Binnacle.Constants.PROTOCOL_LEVEL_5_0 && idx < sizeof(body)) {
                rc = body[idx++];
                if (idx < sizeof(body)) {
                    array(mixed) prop_res = Binnacle.Protocol.Properties.decode_properties(body, idx);
                    disc_props = prop_res[0];
                }
            }
            pkt = Binnacle.Protocol.DisconnectPacket(rc, disc_props);
            break;
        }

        case Binnacle.Constants.PACKET_AUTH: {
            int rc = Binnacle.Constants.RC_SUCCESS;
            mapping(int:mixed) auth_props = ([]);
            if (idx < sizeof(body)) {
                rc = body[idx++];
                if (idx < sizeof(body)) {
                    array(mixed) prop_res = Binnacle.Protocol.Properties.decode_properties(body, idx);
                    auth_props = prop_res[0];
                }
            }
            pkt = Binnacle.Protocol.AuthPacket(rc, auth_props);
            break;
        }

        default:
            throw(Binnacle.Errors.MalformedPacketError(
                sprintf("Unrecognized packet type: %d", packet_type),
                Binnacle.Constants.RC_MALFORMED_PACKET
            ));
    }

    return ({ pkt, total_len });
}
