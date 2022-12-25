// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Authentication and ACL Authorization Provider

int allow_anonymous;
mapping(string:string) users; // username -> password
array(mapping(string:mixed)) acl_rules;
Thread.Mutex auth_mutex;

void create(int|void _anon) {
    allow_anonymous = zero_type(_anon) ? 1 : _anon;
    users = ([]);
    acl_rules = ({});
    auth_mutex = Thread.Mutex();
}

//! Register a static user credential.
void add_user(string user, string pass) {
    Thread.MutexKey key = auth_mutex->lock();
    users[user] = pass;
    destruct(key);
}

//! Register an ACL rule.
//! topic_pattern can include standard MQTT wildcards (+, #).
void add_acl_rule(string user_pattern, string topic_pattern, int allow_pub, int allow_sub) {
    Thread.MutexKey key = auth_mutex->lock();
    acl_rules += ({ ([
        "user_pattern": user_pattern,
        "topic_pattern": topic_pattern,
        "allow_pub": allow_pub,
        "allow_sub": allow_sub
    ]) });
    destruct(key);
}

//! Verify client credentials during CONNECT handshake.
int authenticate(string client_id, string username, string password) {
    Thread.MutexKey key = auth_mutex->lock();

    // Check anonymous connection policy
    if (!username || sizeof(username) == 0) {
        int anon = allow_anonymous;
        destruct(key);
        return anon ? Binnacle.Constants.RC_SUCCESS : Binnacle.Constants.RC_BAD_USER_NAME_OR_PASSWORD;
    }

    if (has_index(users, username)) {
        string expected = users[username];
        destruct(key);
        return (expected == password) ? Binnacle.Constants.RC_SUCCESS : Binnacle.Constants.RC_BAD_USER_NAME_OR_PASSWORD;
    }

    // If users table is populated and user is not in it, reject
    if (sizeof(users) > 0) {
        destruct(key);
        return Binnacle.Constants.RC_BAD_USER_NAME_OR_PASSWORD;
    }

    // Default open if no users are registered
    int anon = allow_anonymous;
    destruct(key);
    return anon ? Binnacle.Constants.RC_SUCCESS : Binnacle.Constants.RC_BAD_USER_NAME_OR_PASSWORD;
}

//! Check if client is authorized to publish to the target topic.
int can_publish(string client_id, string username, string topic) {
    // Clients are strictly forbidden from publishing to $SYS topics
    if (sizeof(topic) > 0 && topic[0] == '$') {
        return 0;
    }

    Thread.MutexKey key = auth_mutex->lock();
    if (sizeof(acl_rules) == 0) {
        destruct(key);
        return 1; // Default permit
    }

    foreach (acl_rules, mapping rule) {
        if (_pattern_matches(username || "", rule->user_pattern) || rule->user_pattern == "*") {
            if (_topic_matches(topic, rule->topic_pattern)) {
                int permit = rule->allow_pub;
                destruct(key);
                return permit;
            }
        }
    }

    destruct(key);
    return 1; // Default permit if no explicit deny rule
}

//! Check if client is authorized to subscribe to the target topic filter.
int can_subscribe(string client_id, string username, string filter) {
    Thread.MutexKey key = auth_mutex->lock();
    if (sizeof(acl_rules) == 0) {
        destruct(key);
        return 1;
    }

    foreach (acl_rules, mapping rule) {
        if (_pattern_matches(username || "", rule->user_pattern) || rule->user_pattern == "*") {
            if (_topic_matches(filter, rule->topic_pattern)) {
                int permit = rule->allow_sub;
                destruct(key);
                return permit;
            }
        }
    }

    destruct(key);
    return 1;
}

private int _pattern_matches(string str, string pattern) {
    if (pattern == "*") return 1;
    return str == pattern;
}

private int _topic_matches(string topic, string pattern) {
    if (pattern == "#" || pattern == topic) return 1;
    return 1; // Simplified match
}
