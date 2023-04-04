# Radix Topic Trie and High-Throughput Routing Engine

## 1. Mathematical Formulation

Let a topic $T$ be an ordered sequence of non-empty UTF-8 strings:
$$T = \langle t_1, t_2, \dots, t_n \rangle \in \Sigma^*, \quad t_i \neq \epsilon$$
separated by the topic level delimiter character `/`.

A topic filter $F = \langle f_1, f_2, \dots, f_m \rangle$ is a sequence of level expressions where each $f_i$ is either:
1. An exact topic string ($f_i = t_i$)
2. A single-level wildcard ($f_i = +$)
3. A multi-level wildcard ($f_i = \#$), which is strictly valid only as the terminal element $f_m$.

A topic $T$ matches a filter $F$ if and only if one of the following conditions is satisfied:
- Length equivalence: $n = m$, and for all $i \in \{1, \dots, n\}$, $(f_i = t_i) \lor (f_i = +)$.
- Multi-level wildcard suffix: $f_m = \#$, with $n \ge m - 1$, and for all $i \in \{1, \dots, m - 1\}$, $(f_i = t_i) \lor (f_i = +)$.

---

## 2. Trie Node Data Structure

Each node in the Binnacle routing trie represents a single topic segment and holds:
- Exact child nodes: `mapping(string:TopicTrieNode)`
- Wildcard child nodes: `single_level_wildcard` (`+`) and `multi_level_wildcard` (`#`)
- Registered client subscriptions: `mapping(string:Subscription)`
- Shared subscription consumer pools: `mapping(string:SharedSubscriptionGroup)`

```
               [Root]
              /   |   \
        sensors  fleet  $SYS
         /   \      \      \
       temp  [+]    [#]   broker
       /       \            \
    living    status       uptime
```

---

## 3. Asymptotic Complexity

| Operation | Best Case | Average Case | Worst Case |
|:---|:---|:---|:---|
| Exact Subscribe | $O(d)$ | $O(d)$ | $O(d)$ |
| Unsubscribe & Prune | $O(d)$ | $O(d)$ | $O(d)$ |
| Exact Route Match | $O(d)$ | $O(d)$ | $O(d)$ |
| Wildcard Route Match | $O(d)$ | $O(d \cdot w)$ | $O(2^d)$ (Degenerate) |

Where $d$ is the depth of the topic (number of `/` segments), and $w$ is the branching factor of wildcards at each level. In real-world telemetry topologies, $d \le 8$, ensuring sub-microsecond matching latency.

---

## 4. Node Pruning Algorithm

To prevent unbounded memory growth during high subscription churn, Binnacle applies an aggressive post-order recursive pruning algorithm:

```pike
private int _prune_node(object node, array(string) segs, int depth, string cid) {
    if (depth == sizeof(segs)) {
        return node->remove_subscription(cid);
    }

    string seg = segs[depth];
    object child = node->get_child(seg);
    if (!child) return 0;

    int removed = _prune_node(child, segs, depth + 1, cid);
    if (child->is_empty()) {
        node->remove_child(seg);
    }
    return removed;
}
```

A node is considered empty if it contains zero local subscriptions, zero shared consumer groups, and zero child branches. Empty branches are deleted immediately, allowing garbage collection to reclaim memory.

---

## 5. Formal Isolation of System Topics ($SYS)

OASIS MQTT Section 4.7.2 states:
> "The Server MUST NOT match Topic Filters that start with a wildcard character ('#' or '+') with Topic Names that begin with a '$' character."

### Verification Proof
Let topic $T = \langle t_1, \dots, t_n \rangle$ satisfy $t_1[0] = '\$'$.
In Binnacle's matching recursion:
1. `is_sys` is evaluated at the root level: `is_sys = (sizeof(topic) > 0 && topic[0] == '$');`.
2. When traversing root children at `depth == 0`:
   - If `is_sys == 1`, branches `child_plus` and `child_hash` are completely bypassed.
   - The router only descends into exact matches matching $t_1$.
3. When `depth > 0`, wildcard matching resumes normally, permitting administrative subscriptions of the form `$SYS/#` or `$SYS/broker/+`.

Therefore, standard consumer wildcards (`#` or `+/temperature`) never receive internal system telemetry, guaranteeing strict tenant boundary isolation.
