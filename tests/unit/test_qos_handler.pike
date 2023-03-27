// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/unit/test_qos_handler.pike
// Description: Unit tests for packet ID pool, rate limiter, and inflight flow control.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("QoS Flow Control and Rate Limiting");

    runner->test("PacketIdPool sequential lease and release", lambda() {
        object pool = Binnacle.Engine.PacketIdPool();
        int id1 = pool->acquire();
        int id2 = pool->acquire();
        int id3 = pool->acquire();

        runner->assert_equal(id1, 1);
        runner->assert_equal(id2, 2);
        runner->assert_equal(id3, 3);

        pool->release(id2);
        runner->assert_false(pool->is_in_use(id2));
        runner->assert_true(pool->is_in_use(id1));
        runner->assert_true(pool->is_in_use(id3));
    });

    runner->test("Token bucket rate limiter consumption and refill", lambda() {
        object limiter = Binnacle.Engine.RateLimiter(10, 10);
        runner->assert_true(limiter->consume(5));
        runner->assert_true(limiter->consume(5));
        // Bucket is now empty
        runner->assert_false(limiter->consume(1));

        // Sleep 1 second to allow token replenishment
        sleep(1);
        runner->assert_true(limiter->consume(1));
    });

    runner->test("InflightWindow receive-maximum capacity bounds", lambda() {
        object win = Binnacle.Engine.InflightWindow(2);
        runner->assert_false(win->is_full());

        object m1 = Binnacle.Protocol.PublishPacket("t1", "p1", 1, 0, 0, 10);
        object m2 = Binnacle.Protocol.PublishPacket("t2", "p2", 1, 0, 0, 11);
        object m3 = Binnacle.Protocol.PublishPacket("t3", "p3", 1, 0, 0, 12);

        win->add(m1);
        runner->assert_false(win->is_full());
        win->add(m2);
        runner->assert_true(win->is_full());

        // Adding m3 must throw QuotaExceededError
        runner->assert_throws(lambda() {
            win->add(m3);
        }, "Adding to full inflight window must throw QuotaExceededError");

        // Acknowledge m1
        object acked = win->acknowledge(10);
        runner->assert_not_null(acked);
        runner->assert_equal(acked->packet_id, 10);
        runner->assert_false(win->is_full());
    });

    return runner->report();
}
