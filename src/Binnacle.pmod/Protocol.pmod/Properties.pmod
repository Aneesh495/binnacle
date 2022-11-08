// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// MQTT 5.0 Property Codec (OASIS MQTT 5.0 Section 2.2.2)

//! Encode a mapping of property identifier to value into the binary property section.
//! Returns binary string including the variable-byte Property Length prefix.
string encode_properties(mapping(int:mixed)|void props) {
    if (!props || sizeof(props) == 0) {
        return sprintf("%c", 0); // Property Length 0
    }

    string body = "";

    foreach (sort(indices(props)), int prop_id) {
        mixed val = props[prop_id];

        switch (prop_id) {
            // Byte properties (1 byte)
            case Binnacle.Constants.PROP_PAYLOAD_FORMAT_INDICATOR:
            case Binnacle.Constants.PROP_REQUEST_PROBLEM_INFORMATION:
            case Binnacle.Constants.PROP_REQUEST_RESPONSE_INFORMATION:
            case Binnacle.Constants.PROP_MAXIMUM_QOS:
            case Binnacle.Constants.PROP_RETAIN_AVAILABLE:
            case Binnacle.Constants.PROP_WILDCARD_SUBSCRIPTION_AVAILABLE:
            case Binnacle.Constants.PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE:
            case Binnacle.Constants.PROP_SHARED_SUBSCRIPTION_AVAILABLE:
                body += sprintf("%c%c", prop_id, (int)val & 0xFF);
                break;

            // Two Byte Integer properties (16-bit big-endian)
            case Binnacle.Constants.PROP_SERVER_KEEP_ALIVE:
            case Binnacle.Constants.PROP_RECEIVE_MAXIMUM:
            case Binnacle.Constants.PROP_TOPIC_ALIAS_MAXIMUM:
            case Binnacle.Constants.PROP_TOPIC_ALIAS:
                body += sprintf("%c%2c", prop_id, (int)val & 0xFFFF);
                break;

            // Four Byte Integer properties (32-bit big-endian)
            case Binnacle.Constants.PROP_MESSAGE_EXPIRY_INTERVAL:
            case Binnacle.Constants.PROP_SESSION_EXPIRY_INTERVAL:
            case Binnacle.Constants.PROP_WILL_DELAY_INTERVAL:
            case Binnacle.Constants.PROP_MAXIMUM_PACKET_SIZE:
                body += sprintf("%c%4c", prop_id, (int)val & 0xFFFFFFFF);
                break;

            // Variable Byte Integer property
            case Binnacle.Constants.PROP_SUBSCRIPTION_IDENTIFIER:
                body += sprintf("%c%s", prop_id, Binnacle.Protocol.VarInt.encode((int)val));
                break;

            // UTF-8 String properties
            case Binnacle.Constants.PROP_CONTENT_TYPE:
            case Binnacle.Constants.PROP_RESPONSE_TOPIC:
            case Binnacle.Constants.PROP_ASSIGNED_CLIENT_IDENTIFIER:
            case Binnacle.Constants.PROP_AUTHENTICATION_METHOD:
            case Binnacle.Constants.PROP_RESPONSE_INFORMATION:
            case Binnacle.Constants.PROP_SERVER_REFERENCE:
            case Binnacle.Constants.PROP_REASON_STRING:
                string s = (string)val;
                body += sprintf("%c%2c%s", prop_id, sizeof(s), s);
                break;

            // Binary Data properties
            case Binnacle.Constants.PROP_CORRELATION_DATA:
            case Binnacle.Constants.PROP_AUTHENTICATION_DATA:
                string b = (string)val;
                body += sprintf("%c%2c%s", prop_id, sizeof(b), b);
                break;

            // User Properties (Array of key-value pairs)
            case Binnacle.Constants.PROP_USER_PROPERTY:
                if (arrayp(val)) {
                    foreach (val, mixed pair) {
                        string k, v;
                        if (arrayp(pair)) {
                            k = pair[0];
                            v = pair[1];
                        } else if (objectp(pair)) {
                            k = pair->key;
                            v = pair->val;
                        }
                        if (k && v) {
                            body += sprintf("%c%2c%s%2c%s",
                                            prop_id, sizeof(k), k, sizeof(v), v);
                        }
                    }
                }
                break;

            default:
                // Unsupported or custom property identifier
                break;
        }
    }

    int body_len = sizeof(body);
    return Binnacle.Protocol.VarInt.encode(body_len) + body;
}

//! Decode binary property section from buffer starting at offset.
//! Returns ({ mapping(int:mixed) properties, int bytes_consumed }).
array(mixed) decode_properties(string buf, int|void offset) {
    int idx = offset || 0;
    int len = sizeof(buf);

    array(int) vi_res = Binnacle.Protocol.VarInt.decode(buf, idx);
    int prop_len = vi_res[0];
    int consumed = vi_res[1];
    idx += consumed;

    mapping(int:mixed) props = ([]);
    int prop_end = idx + prop_len;

    if (prop_end > len) {
        throw(Binnacle.Errors.MalformedPacketError(
            "Truncated property section in packet",
            Binnacle.Constants.RC_MALFORMED_PACKET
        ));
    }

    while (idx < prop_end) {
        int prop_id = buf[idx++];

        switch (prop_id) {
            // Byte properties
            case Binnacle.Constants.PROP_PAYLOAD_FORMAT_INDICATOR:
            case Binnacle.Constants.PROP_REQUEST_PROBLEM_INFORMATION:
            case Binnacle.Constants.PROP_REQUEST_RESPONSE_INFORMATION:
            case Binnacle.Constants.PROP_MAXIMUM_QOS:
            case Binnacle.Constants.PROP_RETAIN_AVAILABLE:
            case Binnacle.Constants.PROP_WILDCARD_SUBSCRIPTION_AVAILABLE:
            case Binnacle.Constants.PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE:
            case Binnacle.Constants.PROP_SHARED_SUBSCRIPTION_AVAILABLE:
                props[prop_id] = buf[idx++];
                break;

            // Two Byte Integer properties
            case Binnacle.Constants.PROP_SERVER_KEEP_ALIVE:
            case Binnacle.Constants.PROP_RECEIVE_MAXIMUM:
            case Binnacle.Constants.PROP_TOPIC_ALIAS_MAXIMUM:
            case Binnacle.Constants.PROP_TOPIC_ALIAS: {
                int val;
                sscanf(buf[idx..idx+1], "%2c", val);
                props[prop_id] = val;
                idx += 2;
                break;
            }

            // Four Byte Integer properties
            case Binnacle.Constants.PROP_MESSAGE_EXPIRY_INTERVAL:
            case Binnacle.Constants.PROP_SESSION_EXPIRY_INTERVAL:
            case Binnacle.Constants.PROP_WILL_DELAY_INTERVAL:
            case Binnacle.Constants.PROP_MAXIMUM_PACKET_SIZE: {
                int val;
                sscanf(buf[idx..idx+3], "%4c", val);
                props[prop_id] = val;
                idx += 4;
                break;
            }

            // Variable Byte Integer property
            case Binnacle.Constants.PROP_SUBSCRIPTION_IDENTIFIER: {
                array(int) sub_vi = Binnacle.Protocol.VarInt.decode(buf, idx);
                props[prop_id] = sub_vi[0];
                idx += sub_vi[1];
                break;
            }

            // UTF-8 String properties
            case Binnacle.Constants.PROP_CONTENT_TYPE:
            case Binnacle.Constants.PROP_RESPONSE_TOPIC:
            case Binnacle.Constants.PROP_ASSIGNED_CLIENT_IDENTIFIER:
            case Binnacle.Constants.PROP_AUTHENTICATION_METHOD:
            case Binnacle.Constants.PROP_RESPONSE_INFORMATION:
            case Binnacle.Constants.PROP_SERVER_REFERENCE:
            case Binnacle.Constants.PROP_REASON_STRING: {
                int str_len;
                sscanf(buf[idx..idx+1], "%2c", str_len);
                idx += 2;
                props[prop_id] = buf[idx..idx+str_len-1];
                idx += str_len;
                break;
            }

            // Binary Data properties
            case Binnacle.Constants.PROP_CORRELATION_DATA:
            case Binnacle.Constants.PROP_AUTHENTICATION_DATA: {
                int bin_len;
                sscanf(buf[idx..idx+1], "%2c", bin_len);
                idx += 2;
                props[prop_id] = buf[idx..idx+bin_len-1];
                idx += bin_len;
                break;
            }

            // User Properties
            case Binnacle.Constants.PROP_USER_PROPERTY: {
                int k_len, v_len;
                sscanf(buf[idx..idx+1], "%2c", k_len);
                idx += 2;
                string k = buf[idx..idx+k_len-1];
                idx += k_len;
                sscanf(buf[idx..idx+1], "%2c", v_len);
                idx += 2;
                string v = buf[idx..idx+v_len-1];
                idx += v_len;

                if (!props[prop_id]) {
                    props[prop_id] = ({});
                }
                props[prop_id] += ({ ({ k, v }) });
                break;
            }

            default:
                throw(Binnacle.Errors.MalformedPacketError(
                    sprintf("Unknown MQTT 5.0 property identifier: 0x%02x", prop_id),
                    Binnacle.Constants.RC_MALFORMED_PACKET
                ));
        }
    }

    int total_bytes = consumed + prop_len;
    return ({ props, total_bytes });
}
