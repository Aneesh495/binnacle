// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Hierarchical Radix Topic Trie Router

object root;
Thread.Mutex trie_mutex;
int total_subscriptions;

void create() {
    root = .TopicTrieNode("");
    trie_mutex = Thread.Mutex();
    total_subscriptions = 0;
}

//! Register a subscription in the topic trie.
void subscribe(object sub) {
    Thread.MutexKey key = trie_mutex->lock();

    string filter = sub->topic_filter;
    string shared_group = 0;
    string actual_filter = filter;

    if (Binnacle.Utils.StringUtil.is_shared_subscription(filter)) {
        array(string)|int parsed = Binnacle.Utils.StringUtil.parse_shared_subscription(filter);
        if (arrayp(parsed)) {
            shared_group = parsed[0];
            actual_filter = parsed[1];
        }
    }

    array(string) segments = actual_filter / "/";
    object curr = root;

    foreach (segments, string seg) {
        curr = curr->get_or_add_child(seg);
    }

    if (shared_group) {
        object g = curr->get_or_create_shared_group(shared_group);
        g->add_subscriber(sub);
    } else {
        curr->add_subscription(sub);
    }

    total_subscriptions++;
    destruct(key);
}

//! Unsubscribe a client from a specific topic filter.
//! Performs recursive node pruning to reclaim unused trie memory.
int unsubscribe(string cid, string filter) {
    Thread.MutexKey key = trie_mutex->lock();

    string shared_group = 0;
    string actual_filter = filter;

    if (Binnacle.Utils.StringUtil.is_shared_subscription(filter)) {
        array(string)|int parsed = Binnacle.Utils.StringUtil.parse_shared_subscription(filter);
        if (arrayp(parsed)) {
            shared_group = parsed[0];
            actual_filter = parsed[1];
        }
    }

    array(string) segments = actual_filter / "/";
    int removed = _prune_node(root, segments, 0, cid, shared_group);

    if (removed && total_subscriptions > 0) {
        total_subscriptions--;
    }

    destruct(key);
    return removed;
}

//! Recursive node traversal and pruning helper.
private int _prune_node(object node, array(string) segs, int depth, string cid, string|void shared_group) {
    if (depth == sizeof(segs)) {
        if (shared_group) {
            node->remove_shared_subscriber(shared_group, cid);
            return 1;
        } else {
            return node->remove_subscription(cid);
        }
    }

    string seg = segs[depth];
    object child = node->get_child(seg);
    if (!child) return 0;

    int removed = _prune_node(child, segs, depth + 1, cid, shared_group);
    if (child->is_empty()) {
        node->remove_child(seg);
    }
    return removed;
}

//! Match a published topic against the trie.
//! Collects all eligible subscriptions, evaluates shared groups, respects NoLocal,
//! and resolves QoS escalation conflicts.
array(object) match(string topic, string|void publisher_cid) {
    Thread.MutexKey key = trie_mutex->lock();

    array(string) segments = topic / "/";
    int is_sys = (sizeof(topic) > 0 && topic[0] == '$');

    mapping(string:object) matched = ([]);
    _match_recursive(root, segments, 0, is_sys, topic, publisher_cid, matched);

    array(object) results = values(matched);
    destruct(key);
    return results;
}

//! Recursive pattern search over trie branches.
private void _match_recursive(object node, array(string) segs, int depth, int is_sys,
                              string topic, string|void publisher_cid, mapping(string:object) matched) {
    // 1. Multi-level wildcard '#' matches everything from this level downward
    // Note: MQTT spec disallows root wildcard '#' matching '$' system topics
    if (node->wildcard_multi && !(depth == 0 && is_sys)) {
        object multi_node = node->wildcard_multi;
        _collect_node_subscriptions(multi_node, topic, publisher_cid, matched);
    }

    // If reached end of topic segments, evaluate terminal matches
    if (depth == sizeof(segs)) {
        _collect_node_subscriptions(node, topic, publisher_cid, matched);
        return;
    }

    string current_seg = segs[depth];

    // 2. Exact match branch
    object exact_child = node->get_child(current_seg);
    if (exact_child) {
        _match_recursive(exact_child, segs, depth + 1, is_sys, topic, publisher_cid, matched);
    }

    // 3. Single-level wildcard '+' branch
    // Note: Root '+' must not match '$' system topics
    if (node->wildcard_single && !(depth == 0 && is_sys)) {
        object single_child = node->wildcard_single;
        _match_recursive(single_child, segs, depth + 1, is_sys, topic, publisher_cid, matched);
    }
}

//! Collect matching subscriptions at a terminal node, resolving duplicates by highest QoS.
private void _collect_node_subscriptions(object node, string topic, string|void publisher_cid,
                                         mapping(string:object) matched) {
    // Standard direct subscriptions
    foreach (values(node->subscriptions), object sub) {
        if (sub->is_suppressed_by_no_local(publisher_cid)) continue;

        string cid = sub->client_id;
        if (!has_index(matched, cid)) {
            matched[cid] = sub;
        } else {
            // If already matched via another filter, escalate to higher QoS
            if (sub->qos > matched[cid]->qos) {
                matched[cid] = sub;
            }
        }
    }

    // Shared subscription groups (dispatch to exactly one selected consumer per group)
    foreach (values(node->shared_groups), object group) {
        object selected = group->select_subscriber(topic, publisher_cid);
        if (selected && !selected->is_suppressed_by_no_local(publisher_cid)) {
            string cid = selected->client_id;
            if (!has_index(matched, cid) || selected->qos > matched[cid]->qos) {
                matched[cid] = selected;
            }
        }
    }
}
