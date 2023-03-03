// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Lightweight Asynchronous HTTP Management and Telemetry Server

object broker;
int port;
string bind_address;
object api;
Stdio.Port listener;
Thread.Mutex http_mutex;

void create(object _broker, int|void _port, string|void _addr) {
    broker = _broker;
    port = zero_type(_port) ? Binnacle.Constants.DEFAULT_HTTP_PORT : _port;
    bind_address = _addr || "0.0.0.0";
    api = .RestApi(broker);
    listener = 0;
    http_mutex = Thread.Mutex();
}

//! Start the HTTP management server.
int start() {
    Thread.MutexKey key = http_mutex->lock();
    listener = Stdio.Port();

    int bound = 0;
    if (bind_address == "0.0.0.0") {
        bound = listener->bind(port, _on_accept);
    } else {
        bound = listener->bind(port, _on_accept, bind_address);
    }

    if (!bound) {
        Binnacle.Utils.Logger.warn("http", sprintf("Failed to bind HTTP management port %d", port));
        listener = 0;
        destruct(key);
        return 0;
    }

    Binnacle.Utils.Logger.info("http", sprintf("HTTP management server active on %s:%d", bind_address, port));
    destruct(key);
    return 1;
}

private void _on_accept(object port_obj) {
    object sock = port_obj->accept();
    if (!sock) return;

    HttpClient(sock, api);
}

void stop() {
    Thread.MutexKey key = http_mutex->lock();
    if (listener) {
        listener->close();
        listener = 0;
    }
    destruct(key);
}

// Inner helper class handling an individual HTTP request/response cycle
class HttpClient {
    object socket;
    object api;
    string buffer;

    void create(object _sock, object _api) {
        socket = _sock;
        api = _api;
        buffer = "";
        socket->set_nonblocking(read_cb, write_cb, close_cb);
    }

    void read_cb(mixed id, string data) {
        if (!data || sizeof(data) == 0) return;
        buffer += data;

        // Check for end of HTTP headers
        int header_end = search(buffer, "\r\n\r\n");
        if (header_end < 0) {
            header_end = search(buffer, "\n\n");
        }
        if (header_end < 0) return;

        string header_part = buffer[0..header_end-1];
        string body_part = buffer[header_end + (search(buffer, "\r\n\r\n") >= 0 ? 4 : 2)..];

        array(string) lines = header_part / "\n";
        if (sizeof(lines) == 0) {
            socket->close();
            return;
        }

        string request_line = String.trim_all_whites(lines[0]);
        array(string) req_tokens = request_line / " ";
        if (sizeof(req_tokens) < 2) {
            socket->close();
            return;
        }

        string method = req_tokens[0];
        string path = req_tokens[1];

        mapping headers = ([]);
        int content_len = 0;
        for (int i = 1; i < sizeof(lines); i++) {
            string l = String.trim_all_whites(lines[i]);
            int colon = search(l, ":");
            if (colon > 0) {
                string hk = lower_case(String.trim_all_whites(l[0..colon-1]));
                string hv = String.trim_all_whites(l[colon+1..]);
                headers[hk] = hv;
                if (hk == "content-length") {
                    sscanf(hv, "%d", content_len);
                }
            }
        }

        if (sizeof(body_part) < content_len) {
            // Need more body bytes
            return;
        }

        array(mixed) resp = api->dispatch(method, path, headers, body_part);
        int status_code = resp[0];
        string ctype = resp[1];
        string resp_body = resp[2];

        string status_text = (status_code == 200) ? "OK" :
                             (status_code == 404) ? "Not Found" :
                             (status_code == 400) ? "Bad Request" : "Internal Server Error";

        string http_resp = sprintf(
            "HTTP/1.1 %d %s\r\n" +
            "Content-Type: %s\r\n" +
            "Content-Length: %d\r\n" +
            "Connection: close\r\n" +
            "Access-Control-Allow-Origin: *\r\n\r\n%s",
            status_code, status_text, ctype, sizeof(resp_body), resp_body
        );

        socket->write(http_resp);
        socket->close();
    }

    void write_cb(mixed id) {}
    void close_cb(mixed id) {}
}
