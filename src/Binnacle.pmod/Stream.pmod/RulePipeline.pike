// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Real-Time Stream Rule Execution Pipeline

object broker;
object dispatcher;
array(object) rules;
Thread.Mutex pipeline_mutex;

void create(object _broker) {
    broker = _broker;
    dispatcher = .ActionDispatcher(broker);
    rules = ({});
    pipeline_mutex = Thread.Mutex();
}

//! Register a new rule in the pipeline.
void add_rule(object rule) {
    Thread.MutexKey key = pipeline_mutex->lock();
    rules += ({ rule });
    destruct(key);
}

//! Process incoming message against all configured pipeline rules.
void process_message(string topic, string raw_payload, mapping(int:mixed)|void props) {
    Thread.MutexKey key = pipeline_mutex->lock();
    if (sizeof(rules) == 0) {
        destruct(key);
        return;
    }

    array(object) rules_snapshot = rules;
    destruct(key);

    // Attempt JSON parse
    mixed parsed_payload = raw_payload;
    catch {
        parsed_payload = Standards.JSON.decode(raw_payload);
    };

    mapping p_map = props || ([]);

    foreach (rules_snapshot, object rule) {
        catch {
            rule->process(topic, parsed_payload, p_map, dispatcher);
        };
    }
}

//! Clear all rules.
void clear() {
    Thread.MutexKey key = pipeline_mutex->lock();
    rules = ({});
    destruct(key);
}

int count() {
    Thread.MutexKey key = pipeline_mutex->lock();
    int c = sizeof(rules);
    destruct(key);
    return c;
}
