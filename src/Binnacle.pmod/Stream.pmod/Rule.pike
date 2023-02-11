// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Stream Analytics Rule Definition

string rule_id;
string topic_filter;
object expression;
array(mapping(string:mixed)) actions;
object aggregator;
string select_field;

void create(string _id, string _filter, string _sql_where,
            array(mapping(string:mixed)) _actions, string|void _select) {
    rule_id = _id;
    topic_filter = _filter;
    expression = .ExpressionParser(_sql_where);
    actions = _actions || ({});
    select_field = _select || "*";
    aggregator = 0;
}

//! Check if published message topic matches rule filter.
int matches_topic(string topic) {
    if (topic_filter == "#" || topic == topic_filter) return 1;

    array(string) t_segs = topic / "/";
    array(string) f_segs = topic_filter / "/";

    int t_len = sizeof(t_segs);
    int f_len = sizeof(f_segs);

    int i = 0;
    while (i < f_len) {
        string f = f_segs[i];
        if (f == "#") return 1;
        if (i >= t_len) return 0;
        if (f != "+" && f != t_segs[i]) return 0;
        i++;
    }
    return i == t_len;
}

//! Evaluate rule against incoming message.
void process(string topic, mixed parsed_payload, mapping props, object dispatcher) {
    if (!matches_topic(topic)) return;

    mapping(string:mixed) ctx = ([
        "topic": topic,
        "payload": parsed_payload,
        "props": props
    ]);

    if (!expression->evaluate(ctx)) {
        return; // Predicate not satisfied
    }

    // Prepare output payload
    string out_payload = "";
    if (select_field == "*") {
        out_payload = stringp(parsed_payload) ? parsed_payload : Standards.JSON.encode(parsed_payload);
    } else {
        mixed selected_val = .JsonPath.get_value(parsed_payload, select_field);
        out_payload = Standards.JSON.encode(([ select_field: selected_val ]));
    }

    // Trigger actions
    foreach (actions, mapping act) {
        string type = act->type;
        if (type == "republish") {
            string target_topic = act->topic || "alerts/" + topic;
            dispatcher->execute_republish(target_topic, out_payload, act->qos || 0, act->retain || 0);
        } else if (type == "log") {
            string path = act->file || "rules.log";
            dispatcher->execute_log(path, sprintf("Rule %s fired on %s: %s", rule_id, topic, out_payload));
        }
    }
}
