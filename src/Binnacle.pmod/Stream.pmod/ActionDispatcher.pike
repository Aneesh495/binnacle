// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Stream Rule Action Dispatcher

object broker;

void create(object _broker) {
    broker = _broker;
}

//! Republish transformed message to another topic.
void execute_republish(string target_topic, string payload, int qos, int retain) {
    if (broker) {
        broker->internal_publish(target_topic, payload, qos, retain);
    }
}

//! Log transformed message to disk file.
void execute_log(string file_path, string msg) {
    Stdio.File f = Stdio.File();
    if (f->open(file_path, "cwa")) {
        f->write(sprintf("[%s] %s\n", Binnacle.Utils.Time.format_iso8601(), msg));
        f->close();
    }
}
