// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/unit/test_offline_queue.pike
// Description: Unit tests for bounded offline message queue and eviction strategies.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Offline Message Queue and Eviction Policies");

    runner->test("FIFO ordering and dequeue", lambda() {
        object q = Binnacle.Session.OfflineQueue(10, "drop_oldest");
        object m1 = Binnacle.Protocol.PublishPacket("t/1", "payload-1", 1, 0, 0, 1);
        object m2 = Binnacle.Protocol.PublishPacket("t/2", "payload-2", 1, 0, 0, 2);

        q->enqueue(m1);
        q->enqueue(m2);

        runner->assert_equal(q->size(), 2);
        object d1 = q->dequeue();
        runner->assert_equal(d1->payload, "payload-1");
        object d2 = q->dequeue();
        runner->assert_equal(d2->payload, "payload-2");
        runner->assert_equal(q->size(), 0);
        runner->assert_null(q->dequeue());
    });

    runner->test("Capacity overflow with drop_oldest policy", lambda() {
        object q = Binnacle.Session.OfflineQueue(2, "drop_oldest");
        object m1 = Binnacle.Protocol.PublishPacket("t/1", "first", 1, 0, 0, 1);
        object m2 = Binnacle.Protocol.PublishPacket("t/2", "second", 1, 0, 0, 2);
        object m3 = Binnacle.Protocol.PublishPacket("t/3", "third", 1, 0, 0, 3);

        q->enqueue(m1);
        q->enqueue(m2);
        runner->assert_equal(q->size(), 2);

        // Enqueueing m3 drops m1 (oldest)
        q->enqueue(m3);
        runner->assert_equal(q->size(), 2);

        object d1 = q->dequeue();
        runner->assert_equal(d1->payload, "second");
        object d2 = q->dequeue();
        runner->assert_equal(d2->payload, "third");
    });

    runner->test("Capacity overflow with drop_newest policy", lambda() {
        object q = Binnacle.Session.OfflineQueue(2, "drop_newest");
        object m1 = Binnacle.Protocol.PublishPacket("t/1", "first", 1, 0, 0, 1);
        object m2 = Binnacle.Protocol.PublishPacket("t/2", "second", 1, 0, 0, 2);
        object m3 = Binnacle.Protocol.PublishPacket("t/3", "third", 1, 0, 0, 3);

        q->enqueue(m1);
        q->enqueue(m2);

        // Enqueueing m3 should be rejected because queue is full
        int accepted = q->enqueue(m3);
        runner->assert_equal(accepted, 0);
        runner->assert_equal(q->size(), 2);

        object d1 = q->dequeue();
        runner->assert_equal(d1->payload, "first");
        object d2 = q->dequeue();
        runner->assert_equal(d2->payload, "second");
    });

    return runner->report();
}
