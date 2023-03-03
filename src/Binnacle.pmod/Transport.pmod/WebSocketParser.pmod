// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// RFC 6455 WebSocket Handshake and Frame Parser

constant WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

//! Generate HTTP 101 Switching Protocols response for WebSocket upgrade request.
string|int generate_handshake_response(string http_request) {
    int key_idx = search(http_request, "Sec-WebSocket-Key:");
    if (key_idx < 0) return 0;

    int line_end = search(http_request, "\r\n", key_idx);
    if (line_end < 0) line_end = search(http_request, "\n", key_idx);
    if (line_end < 0) return 0;

    string client_key = String.trim_all_whites(http_request[key_idx + 18 .. line_end - 1]);
    string to_hash = client_key + WS_GUID;

    string sha1_raw = Crypto.SHA1.hash(to_hash);
    string accept_key = String.trim_all_whites(MIME.encode_base64(sha1_raw));

    return "HTTP/1.1 101 Switching Protocols\r\n" +
           "Upgrade: websocket\r\n" +
           "Connection: Upgrade\r\n" +
           "Sec-WebSocket-Accept: " + accept_key + "\r\n" +
           "Sec-WebSocket-Protocol: mqtt\r\n\r\n";
}

//! Decode a WebSocket frame from buffer.
//! Returns ({ object frame, int bytes_consumed }) or 0 if incomplete.
array(mixed)|int decode_frame(string buf) {
    int len = sizeof(buf);
    if (len < 2) return 0;

    int b1 = buf[0];
    int fin = (b1 & 0x80) ? 1 : 0;
    int opcode = b1 & 0x0F;

    int b2 = buf[1];
    int has_mask = (b2 & 0x80) ? 1 : 0;
    int payload_len = b2 & 0x7F;
    int idx = 2;

    if (payload_len == 126) {
        if (len < 4) return 0;
        sscanf(buf[2..3], "%2c", payload_len);
        idx = 4;
    } else if (payload_len == 127) {
        if (len < 10) return 0;
        sscanf(buf[2..9], "%8c", payload_len);
        idx = 10;
    }

    string mask_key = "";
    if (has_mask) {
        if (len < (idx + 4)) return 0;
        mask_key = buf[idx..idx+3];
        idx += 4;
    }

    if (len < (idx + payload_len)) return 0;

    string raw_payload = buf[idx..idx+payload_len-1];
    idx += payload_len;

    string final_payload = "";
    if (has_mask) {
        // Unmask bytes
        array(int) unmasked = allocate(payload_len);
        for (int i = 0; i < payload_len; i++) {
            unmasked[i] = raw_payload[i] ^ mask_key[i % 4];
        }
        final_payload = sprintf("%{%c%}", unmasked);
    } else {
        final_payload = raw_payload;
    }

    object frame = .WebSocketFrame(opcode, final_payload, fin);
    return ({ frame, idx });
}
