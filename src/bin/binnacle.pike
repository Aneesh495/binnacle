#!/usr/bin/env pike
// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Main Daemon Entrypoint

int main(int argc, array(string) argv) {
    string config_file = "binnacle.conf.example";
    int override_port = 0;
    int override_http_port = 0;
    int override_ws_port = 0;
    string log_level = "info";

    // Command line argument parser
    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if (arg == "--config" && (i + 1) < argc) {
            config_file = argv[++i];
        } else if (arg == "--port" && (i + 1) < argc) {
            sscanf(argv[++i], "%d", override_port);
        } else if (arg == "--http-port" && (i + 1) < argc) {
            sscanf(argv[++i], "%d", override_http_port);
        } else if (arg == "--ws-port" && (i + 1) < argc) {
            sscanf(argv[++i], "%d", override_ws_port);
        } else if (arg == "--log-level" && (i + 1) < argc) {
            log_level = argv[++i];
        } else if (arg == "--help" || arg == "-h") {
            write("Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric\n");
            write("Usage: pike -Msrc src/bin/binnacle.pike [OPTIONS]\n\n");
            write("Options:\n");
            write("  --config <file>     Path to configuration file (default: binnacle.conf.example)\n");
            write("  --port <port>       MQTT TCP listener port (default: 1883)\n");
            write("  --http-port <port>  HTTP management and metrics port (default: 8080)\n");
            write("  --ws-port <port>    WebSocket listener port (default: 8083)\n");
            write("  --log-level <level> Logging level: debug, info, warn, error (default: info)\n");
            write("  --help, -h          Display this help manual\n");
            return 0;
        }
    }

    // Load configuration
    object cfg = Binnacle.Utils.ConfigParser.parse_file(config_file);
    if (override_port > 0) cfg->set("port", override_port);
    if (override_http_port > 0) cfg->set("http_port", override_http_port);
    if (override_ws_port > 0) cfg->set("websocket_port", override_ws_port);

    int port = cfg->get_int("port", 1883);
    int http_port = cfg->get_int("http_port", 8080);
    int ws_port = cfg->get_int("websocket_port", 8083);
    string bind_addr = cfg->get_string("bind_address", "0.0.0.0");
    string node_id = cfg->get_string("node_id", "binnacle-01");
    string storage_dir = cfg->get_string("storage_directory", "data/wal");

    // Initialize logger
    Binnacle.Utils.Logger.init(Binnacle.Utils.Logger.parse_level(log_level), "text");

    // Banner display
    write("====================================================================\n");
    write("  ____  ___ _   _ _   _    _    ____ _     _____\n");
    write(" | __ )|_ _| \\ | | \\ | |  / \\  / ___| |   | ____|\n");
    write(" |  _ \\ | ||  \\| |  \\| | / _ \\| |   | |   |  _|\n");
    write(" | |_) || || |\\  | |\\  |/ ___ \\ |___| |___| |___\n");
    write(" |____/|___|_| \\_|_| \\_/_/   \\_\\____|_____|_____|\n");
    write(" Distributed MQTT 5.0 Telemetry Broker & Message Fabric\n");
    write("====================================================================\n");
    write(" Node ID         : %s\n", node_id);
    write(" MQTT TCP Port   : %s:%d\n", bind_addr, port);
    write(" HTTP Telemetry  : http://%s:%d/dashboard\n", bind_addr == "0.0.0.0" ? "localhost" : bind_addr, http_port);
    write(" Prometheus Path : http://%s:%d/metrics\n", bind_addr == "0.0.0.0" ? "localhost" : bind_addr, http_port);
    write(" WebSocket Port  : ws://%s:%d/mqtt\n", bind_addr == "0.0.0.0" ? "localhost" : bind_addr, ws_port);
    write(" Storage Engine  : %s\n", storage_dir);
    write("====================================================================\n");

    // Initialize broker core
    object broker = Binnacle.Engine.BrokerServer(cfg);

    // Initialize storage if enabled
    if (cfg->get_bool("storage_enabled", 1)) {
        object storage = Binnacle.Storage.StorageEngine(broker, storage_dir, cfg->get_int("compaction_interval_seconds", 300));
        storage->start();
        broker->storage_engine = storage;
    }

    // Start broker engine
    broker->start();

    // Start TCP ingress
    object tcp = Binnacle.Transport.TcpServer(broker, bind_addr, port);
    if (!tcp->start()) {
        werror("Failed to start TCP listener. Exiting.\n");
        return 1;
    }

    // Start HTTP management server
    if (cfg->get_bool("http_enabled", 1)) {
        object http = Binnacle.Admin.HttpServer(broker, http_port, bind_addr);
        http->start();
    }

    // Start WebSocket transport
    if (cfg->get_bool("websocket_enabled", 1)) {
        object ws = Binnacle.Transport.WebSocketTransport(broker, bind_addr, ws_port);
        ws->start();
    }

    Binnacle.Utils.Logger.info("main", "Binnacle daemon running. Press Ctrl+C to terminate.");
    return -1; // Keep event loop running indefinitely
}
