// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/regressions/test_bug_session_expiry_cleanup.pike
// Description: Regression test ensuring session expiry accurately detects expired sessions when current time parameter is omitted.

class MockActor {}

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Regression: Session Expiry Parameter Default Handling");

    runner->test("Bug Fix: is_expired() evaluates against wall clock time when argument omitted", lambda() {
        // Create session with 2 second expiry interval
        object sess = Binnacle.Session.Session("client-reg-01", 2, 100);
        sess->attach_connection(MockActor());
        sess->detach_connection();

        // Freshly disconnected session is NOT expired
        runner->assert_false(sess->is_expired());

        // Simulate elapsed time past expiry interval
        sess->disconnected_at = time() - 10; // 10 seconds ago

        // Calling is_expired() with no argument MUST default to current wall clock time and return 1 (expired)
        runner->assert_true(sess->is_expired(), "Session past expiry interval must evaluate as expired");
    });

    runner->test("Bug Fix: Active sessions never expire even if disconnected_at was historical", lambda() {
        object sess = Binnacle.Session.Session("client-reg-02", 2, 100);
        sess->attach_connection(MockActor());
        sess->disconnected_at = time() - 100;

        runner->assert_false(sess->is_expired(), "Active connected session must never expire");
    });

    return runner->report();
}
