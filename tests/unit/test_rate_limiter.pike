// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/unit/test_rate_limiter.pike
// Description: Unit tests for token bucket traffic shaping and rate limiting.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Token Bucket Rate Limiter");

    runner->test("Bucket capacity and burst control", lambda() {
        object limiter = Binnacle.Engine.RateLimiter(50, 100);
        runner->assert_equal((int)limiter->get_available_tokens(), 100);

        // Consume 60 tokens from 100 burst capacity
        runner->assert_true(limiter->consume(60));
        runner->assert_true(limiter->get_available_tokens() <= 40.0);
    });

    runner->test("Limiter throttling on token exhaustion", lambda() {
        object limiter = Binnacle.Engine.RateLimiter(10, 10);
        runner->assert_true(limiter->consume(10));
        // Next consumption should be throttled
        runner->assert_false(limiter->consume(1));
    });

    runner->test("Rate limit disabled when configured to zero", lambda() {
        object limiter = Binnacle.Engine.RateLimiter(0, 0);
        // Should always permit arbitrarily large requests
        runner->assert_true(limiter->consume(1000000));
        runner->assert_true(limiter->consume(5000000));
    });

    return runner->report();
}
