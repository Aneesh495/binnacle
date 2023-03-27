// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/unit/test_shared_subs.pike
// Description: Unit tests for shared subscriptions load-balancing dispatch.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Shared Subscription Load Balancing");

    runner->test("Round-robin load balancing across consumer pool", lambda() {
        object group = Binnacle.Router.SharedSubscriptionGroup("workers", "round_robin");
        object s1 = Binnacle.Router.Subscription("worker-01", "$share/workers/jobs", 1);
        object s2 = Binnacle.Router.Subscription("worker-02", "$share/workers/jobs", 1);
        object s3 = Binnacle.Router.Subscription("worker-03", "$share/workers/jobs", 1);

        group->add_subscriber(s1);
        group->add_subscriber(s2);
        group->add_subscriber(s3);

        object picked1 = group->select_subscriber("jobs/task1");
        object picked2 = group->select_subscriber("jobs/task2");
        object picked3 = group->select_subscriber("jobs/task3");
        object picked4 = group->select_subscriber("jobs/task4");

        runner->assert_equal(picked1->client_id, "worker-01");
        runner->assert_equal(picked2->client_id, "worker-02");
        runner->assert_equal(picked3->client_id, "worker-03");
        runner->assert_equal(picked4->client_id, "worker-01"); // Wrapped around
    });

    runner->test("Dynamic subscriber removal during load-balancing", lambda() {
        object group = Binnacle.Router.SharedSubscriptionGroup("fleet-processors", "round_robin");
        object s1 = Binnacle.Router.Subscription("w1", "$share/fleet/telemetry", 0);
        object s2 = Binnacle.Router.Subscription("w2", "$share/fleet/telemetry", 0);

        group->add_subscriber(s1);
        group->add_subscriber(s2);
        runner->assert_equal(group->select_subscriber("telemetry")->client_id, "w1");

        // Remove w1
        group->remove_subscriber("w1");
        runner->assert_equal(group->select_subscriber("telemetry")->client_id, "w2");
        runner->assert_equal(group->select_subscriber("telemetry")->client_id, "w2");
    });

    runner->test("Sticky-hash consistent affinity", lambda() {
        object group = Binnacle.Router.SharedSubscriptionGroup("stateful-workers", "sticky_hash");
        object s1 = Binnacle.Router.Subscription("node-A", "$share/stateful-workers/vin/+", 1);
        object s2 = Binnacle.Router.Subscription("node-B", "$share/stateful-workers/vin/+", 1);

        group->add_subscriber(s1);
        group->add_subscriber(s2);

        // Same publisher / topic must always hit the same worker
        object first = group->select_subscriber("vin/12345", "device-alpha");
        for (int i = 0; i < 5; i++) {
            object target = group->select_subscriber("vin/12345", "device-alpha");
            runner->assert_equal(target->client_id, first->client_id);
        }
    });

    return runner->report();
}
