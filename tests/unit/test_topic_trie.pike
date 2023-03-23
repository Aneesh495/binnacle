// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/unit/test_topic_trie.pike
// Description: Unit tests for radix topic trie, wildcard resolution, and $SYS isolation.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Radix Topic Trie and Router");

    runner->test("Exact topic filter match and dispatch", lambda() {
        object trie = Binnacle.Router.TopicTrie();
        object sub1 = Binnacle.Router.Subscription("client-01", "sensors/temp", 1);
        object sub2 = Binnacle.Router.Subscription("client-02", "sensors/pressure", 0);

        trie->subscribe(sub1);
        trie->subscribe(sub2);

        array matched = trie->match("sensors/temp");
        runner->assert_equal(sizeof(matched), 1);
        runner->assert_equal(matched[0]->client_id, "client-01");

        array matched2 = trie->match("sensors/pressure");
        runner->assert_equal(sizeof(matched2), 1);
        runner->assert_equal(matched2[0]->client_id, "client-02");

        array matched3 = trie->match("sensors/humidity");
        runner->assert_equal(sizeof(matched3), 0);
    });

    runner->test("Single-level wildcard '+' matching", lambda() {
        object trie = Binnacle.Router.TopicTrie();
        object sub = Binnacle.Router.Subscription("client-wild-plus", "sensors/+/temperature", 1);
        trie->subscribe(sub);

        array m1 = trie->match("sensors/room1/temperature");
        runner->assert_equal(sizeof(m1), 1);

        array m2 = trie->match("sensors/room2/temperature");
        runner->assert_equal(sizeof(m2), 1);

        // Does not match different depth or missing middle
        array m3 = trie->match("sensors/temperature");
        runner->assert_equal(sizeof(m3), 0);

        array m4 = trie->match("sensors/room1/floor2/temperature");
        runner->assert_equal(sizeof(m4), 0);
    });

    runner->test("Multi-level wildcard '#' matching", lambda() {
        object trie = Binnacle.Router.TopicTrie();
        object sub = Binnacle.Router.Subscription("client-wild-hash", "sensors/#", 2);
        trie->subscribe(sub);

        runner->assert_equal(sizeof(trie->match("sensors/temp")), 1);
        runner->assert_equal(sizeof(trie->match("sensors/living/temp")), 1);
        runner->assert_equal(sizeof(trie->match("sensors/living/floor/room/temp")), 1);
        runner->assert_equal(sizeof(trie->match("actuators/switch")), 0);
    });

    runner->test("Root multi-level wildcard '#' matches all non-system topics", lambda() {
        object trie = Binnacle.Router.TopicTrie();
        object sub = Binnacle.Router.Subscription("client-all", "#", 0);
        trie->subscribe(sub);

        runner->assert_equal(sizeof(trie->match("a/b/c")), 1);
        runner->assert_equal(sizeof(trie->match("foo")), 1);
    });

    runner->test("$SYS system topic isolation from wildcards", lambda() {
        object trie = Binnacle.Router.TopicTrie();
        object sub_all = Binnacle.Router.Subscription("client-all", "#", 0);
        object sub_plus = Binnacle.Router.Subscription("client-plus", "+/broker/uptime", 0);
        object sub_sys = Binnacle.Router.Subscription("client-sys", "$SYS/#", 0);

        trie->subscribe(sub_all);
        trie->subscribe(sub_plus);
        trie->subscribe(sub_sys);

        // $SYS must not match '#' or '+' at root level per MQTT specification
        array res = trie->match("$SYS/broker/uptime");
        runner->assert_equal(sizeof(res), 1);
        runner->assert_equal(res[0]->client_id, "client-sys");
    });

    runner->test("NoLocal subscription option suppression", lambda() {
        object trie = Binnacle.Router.TopicTrie();
        // Client-01 subscribes with no_local = 1
        object sub = Binnacle.Router.Subscription("client-01", "chat/room", 1, 1);
        trie->subscribe(sub);

        // Published by client-01: should be suppressed
        array suppressed = trie->match("chat/room", "client-01");
        runner->assert_equal(sizeof(suppressed), 0);

        // Published by client-02: should match
        array delivered = trie->match("chat/room", "client-02");
        runner->assert_equal(sizeof(delivered), 1);
    });

    runner->test("Unsubscribe and node pruning", lambda() {
        object trie = Binnacle.Router.TopicTrie();
        object sub = Binnacle.Router.Subscription("client-01", "alpha/beta/gamma", 1);
        trie->subscribe(sub);

        runner->assert_equal(sizeof(trie->match("alpha/beta/gamma")), 1);

        int removed = trie->unsubscribe("client-01", "alpha/beta/gamma");
        runner->assert_equal(removed, 1);
        runner->assert_equal(sizeof(trie->match("alpha/beta/gamma")), 0);
    });

    return runner->report();
}
