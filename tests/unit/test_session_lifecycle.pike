// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/unit/test_session_lifecycle.pike
// Description: Unit tests for client session lifecycle, CleanStart semantics, and expiry.

class MockActor {}

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Client Session Lifecycle and Registry");

    runner->test("Session initialization and active state transitions", lambda() {
        object sess = Binnacle.Session.Session("client-test-01", 300, 65535);
        runner->assert_equal(sess->client_id, "client-test-01");
        runner->assert_equal(sess->session_expiry_interval, 300);
        runner->assert_equal(sess->is_active, 0);

        sess->attach_connection(MockActor());
        runner->assert_equal(sess->is_active, 1);
        runner->assert_equal(sess->disconnected_at, 0);

        sess->detach_connection();
        runner->assert_equal(sess->is_active, 0);
        runner->assert_true(sess->disconnected_at > 0);
    });

    runner->test("Session Expiry Interval calculation", lambda() {
        object sess = Binnacle.Session.Session("client-exp-01", 2, 65535);
        sess->attach_connection(MockActor());
        runner->assert_false(sess->is_expired(), "Active session must never expire");

        sess->detach_connection();
        runner->assert_false(sess->is_expired(), "Freshly disconnected session is not expired yet");

        // Simulate passage of time
        sess->disconnected_at = time() - 5; // 5s ago (> 2s interval)
        runner->assert_true(sess->is_expired(), "Session past expiry interval must report expired");
    });

    runner->test("SessionManager CleanStart semantics", lambda() {
        object mgr = Binnacle.Session.SessionManager(0);

        // First connect with CleanStart=1
        array res1 = mgr->get_or_create_session("client-sess-01", 1, 3600, 100);
        object s1 = res1[0];
        int sp1 = res1[1];
        runner->assert_equal(sp1, 0, "Initial connection must have session_present=0");

        // Reconnect with CleanStart=0 (Resume)
        array res2 = mgr->get_or_create_session("client-sess-01", 0, 3600, 100);
        int sp2 = res2[1];
        runner->assert_equal(sp2, 1, "Resumed session must have session_present=1");

        // Reconnect with CleanStart=1 (Reset)
        array res3 = mgr->get_or_create_session("client-sess-01", 1, 3600, 100);
        int sp3 = res3[1];
        runner->assert_equal(sp3, 0, "CleanStart=1 must reset session_present=0");
    });

    return runner->report();
}
