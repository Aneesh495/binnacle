#!/usr/bin/env pike
// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Administrative Control CLI

int main(int argc, array(string) argv) {
    string host = "127.0.0.1";
    int port = 8080;
    string command = "stats";

    if (argc < 2) {
        write("Binnacle Admin CLI\n");
        write("Usage: pike -Msrc src/bin/binnacle_admin.pike <command> [OPTIONS]\n\n");
        write("Commands:\n");
        write("  stats                 Display live broker metrics\n");
        write("  topics                List active retained topics\n");
        write("  publish <t> <msg>     Publish message via REST API\n");
        write("  health                Query health status\n");
        return 0;
    }

    command = argv[1];

    Protocols.HTTP.Query q;
    string base_url = sprintf("http://%s:%d", host, port);

    if (command == "health") {
        q = Protocols.HTTP.get_url(base_url + "/health");
        write("Response: %s\n", q->data());
    } else if (command == "stats") {
        q = Protocols.HTTP.get_url(base_url + "/api/v1/stats");
        mixed data = Standards.JSON.decode(q->data());
        write("========================================\n");
        write("  Binnacle Cluster Status: %s\n", data->node_id);
        write("========================================\n");
        write("  Version             : %s\n", data->version);
        write("  Active Connections  : %d\n", data->clients_connected);
        write("  Total Sessions      : %d\n", data->clients_total);
        write("  Topic Subscriptions : %d\n", data->subscriptions_count);
        write("  Retained Topics     : %d\n", data->retained_count);
        write("  Messages Ingested   : %d\n", data->messages_received);
        write("  Messages Dispatched : %d\n", data->messages_sent);
        write("========================================\n");
    } else if (command == "topics") {
        q = Protocols.HTTP.get_url(base_url + "/api/v1/topics");
        array(mixed) list = Standards.JSON.decode(q->data());
        write("%-35s | %-4s | %s\n", "Topic", "QoS", "Payload Preview");
        write("--------------------------------------------------------------------\n");
        foreach (list, mapping item) {
            write("%-35s | QoS %d | %s\n", item->topic, item->qos, item->payload_preview);
        }
    } else if (command == "publish" && argc >= 4) {
        string t = argv[2];
        string m = argv[3];
        mapping body_m = ([ "topic": t, "payload": m, "qos": 0 ]);
        string json_body = Standards.JSON.encode(body_m);

        q = Protocols.HTTP.post_url(base_url + "/api/v1/publish", json_body, ([
            "Content-Type": "application/json"
        ]));
        write("Published: %s\n", q->data());
    } else {
        werror("Unknown command: %s\n", command);
        return 1;
    }

    return 0;
}
