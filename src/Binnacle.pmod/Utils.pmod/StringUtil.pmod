// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// UTF-8 and Topic String Validation Primitives

//! Validate whether a string is well-formed UTF-8 without forbidden MQTT characters.
int is_valid_mqtt_utf8(string s) {
    if (!s) return 0;
    int len = sizeof(s);
    if (len > 65535) return 0;

    for (int i = 0; i < len; i++) {
        int ch = s[i];
        // MQTT prohibits U+0000 (null byte) in UTF-8 strings
        if (ch == 0) return 0;
    }
    return 1;
}

//! Validate a topic name used in a PUBLISH packet.
//! Must not be empty, must not contain '+' or '#', and must be valid UTF-8.
int is_valid_topic_name(string topic) {
    if (!topic || sizeof(topic) == 0) return 0;
    if (sizeof(topic) > 65535) return 0;
    if (!is_valid_mqtt_utf8(topic)) return 0;

    if (search(topic, "+") >= 0 || search(topic, "#") >= 0) {
        return 0; // Wildcards not permitted in publish topic names
    }
    return 1;
}

//! Validate a topic filter used in a SUBSCRIBE packet.
//! Wildcards '+' and '#' must adhere strictly to MQTT 5.0 rules:
//! '#' must be the last character and preceded by '/' (or be the single character '#').
//! '+' must occupy an entire level (e.g. '+/a', 'a/+', 'a/+/b', or single '+').
int is_valid_topic_filter(string filter) {
    if (!filter || sizeof(filter) == 0) return 0;
    if (sizeof(filter) > 65535) return 0;
    if (!is_valid_mqtt_utf8(filter)) return 0;

    // Check multi-level wildcard '#'
    int hash_pos = search(filter, "#");
    if (hash_pos >= 0) {
        // Must be the very last character
        if (hash_pos != sizeof(filter) - 1) return 0;
        // If not the first character, preceding character must be '/'
        if (hash_pos > 0 && filter[hash_pos - 1] != '/') return 0;
    }

    // Check single-level wildcard '+'
    array(string) levels = filter / "/";
    foreach (levels, string lvl) {
        if (search(lvl, "+") >= 0 && lvl != "+") {
            return 0; // '+' cannot be mixed with other characters in the same level
        }
    }

    return 1;
}

//! Split a topic into level segments.
array(string) split_topic(string topic) {
    if (!topic || sizeof(topic) == 0) return ({});
    return topic / "/";
}

//! Encode string into MQTT 2-byte length-prefixed format.
string encode_string(string s) {
    int len = sizeof(s);
    return sprintf("%2c%s", len, s);
}

//! Encode binary data into MQTT 2-byte length-prefixed format.
string encode_binary(string data) {
    int len = sizeof(data);
    return sprintf("%2c%s", len, data);
}

//! Check if topic is a shared subscription (starts with $share/<group>/...).
int is_shared_subscription(string filter) {
    return has_prefix(filter, "$share/");
}

//! Parse shared subscription filter into group name and actual topic filter.
//! Returns ({ group, actual_topic_filter }) or 0 if malformed.
array(string)|int parse_shared_subscription(string filter) {
    if (!is_shared_subscription(filter)) return 0;
    // Format: $share/{group}/{topic_filter}
    string rest = filter[7..];
    int slash_idx = search(rest, "/");
    if (slash_idx <= 0 || slash_idx == sizeof(rest) - 1) {
        return 0; // Missing group or topic
    }
    string group = rest[0..slash_idx - 1];
    string actual_topic = rest[slash_idx + 1..];
    if (sizeof(group) == 0 || !is_valid_topic_filter(actual_topic)) {
        return 0;
    }
    return ({ group, actual_topic });
}
