// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Radix Topic Trie Node Representation

string token;
mapping(string:object) children;
object wildcard_single; // Fast-path cached '+' child
object wildcard_multi;  // Fast-path cached '#' child
mapping(string:object) subscriptions; // client_id -> Subscription
mapping(string:object) shared_groups; // group_name -> SharedSubscriptionGroup

void create(string|void _token) {
    token = _token || "";
    children = ([]);
    wildcard_single = 0;
    wildcard_multi = 0;
    subscriptions = ([]);
    shared_groups = ([]);
}

//! Retrieve child node matching token.
object get_child(string seg) {
    if (has_index(children, seg)) {
        return children[seg];
    }
    return 0;
}

//! Add or retrieve child node for topic segment.
object get_or_add_child(string seg) {
    if (has_index(children, seg)) {
        return children[seg];
    }
    object child = .TopicTrieNode(seg);
    children[seg] = child;
    if (seg == "+") {
        wildcard_single = child;
    } else if (seg == "#") {
        wildcard_multi = child;
    }
    return child;
}

//! Remove a child node by segment token.
void remove_child(string seg) {
    if (has_index(children, seg)) {
        m_delete(children, seg);
        if (seg == "+") wildcard_single = 0;
        if (seg == "#") wildcard_multi = 0;
    }
}

//! Register a subscription on this node.
void add_subscription(object sub) {
    subscriptions[sub->client_id] = sub;
}

//! Deregister a subscription by client ID.
int remove_subscription(string cid) {
    if (has_index(subscriptions, cid)) {
        m_delete(subscriptions, cid);
        return 1;
    }
    return 0;
}

//! Register or retrieve a shared subscription group on this node.
object get_or_create_shared_group(string group_name) {
    if (has_index(shared_groups, group_name)) {
        return shared_groups[group_name];
    }
    object g = .SharedSubscriptionGroup(group_name);
    shared_groups[group_name] = g;
    return g;
}

//! Remove a subscriber from a shared subscription group.
void remove_shared_subscriber(string group_name, string cid) {
    if (has_index(shared_groups, group_name)) {
        object g = shared_groups[group_name];
        g->remove_subscriber(cid);
        if (g->is_empty()) {
            m_delete(shared_groups, group_name);
        }
    }
}

//! Check if this node is completely empty and eligible for branch pruning.
int is_empty() {
    return sizeof(children) == 0 &&
           sizeof(subscriptions) == 0 &&
           sizeof(shared_groups) == 0;
}
