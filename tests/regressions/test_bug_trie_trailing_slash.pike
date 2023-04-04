// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/regressions/test_bug_trie_trailing_slash.pike
// Description: Regression test ensuring strict level hierarchy separation for trailing slashes.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Regression: Topic Trie Trailing Slash Hierarchy");

    runner->test("Bug Fix: Trailing slashes designate distinct hierarchy levels", lambda() {
        object trie = Binnacle.Router.TopicTrie();
        object sub_exact_no_slash = Binnacle.Router.Subscription("sub-01", "building/floor", 0);
        object sub_exact_with_slash = Binnacle.Router.Subscription("sub-02", "building/floor/", 0);

        trie->subscribe(sub_exact_no_slash);
        trie->subscribe(sub_exact_with_slash);

        // Topic without slash matches sub-01, but NOT sub-02
        array m1 = trie->match("building/floor");
        runner->assert_equal(sizeof(m1), 1);
        runner->assert_equal(m1[0]->client_id, "sub-01");

        // Topic with slash matches sub-02, but NOT sub-01
        array m2 = trie->match("building/floor/");
        runner->assert_equal(sizeof(m2), 1);
        runner->assert_equal(m2[0]->client_id, "sub-02");
    });

    runner->test("Bug Fix: Single level '+' wildcard matches empty level trailing slash", lambda() {
        object trie = Binnacle.Router.TopicTrie();
        object sub_wild = Binnacle.Router.Subscription("sub-wild", "building/+", 0);
        trie->subscribe(sub_wild);

        // "building/" has empty second level -> matches "building/+"
        array m1 = trie->match("building/");
        runner->assert_equal(sizeof(m1), 1);

        // "building" has only 1 level -> does NOT match "building/+"
        array m2 = trie->match("building");
        runner->assert_equal(sizeof(m2), 0);
    });

    return runner->report();
}
