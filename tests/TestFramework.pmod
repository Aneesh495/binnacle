// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/TestFramework.pmod
// Description: Structured unit and integration test framework for Pike test suites.

class TestRunner {
    int passed = 0;
    int failed = 0;
    string suite_name = "";
    int suite_start_time = 0;

    void start_suite(string name) {
        suite_name = name;
        suite_start_time = time();
        write("\n=== Running Suite: %s ===\n", suite_name);
    }

    void test(string case_name, function fn) {
        mixed err = catch {
            fn();
        };

        if (err) {
            failed++;
            write("  [FAIL] %s\n", case_name);
            if (stringp(err)) {
                write("         Reason: %s\n", err);
            } else if (arrayp(err) && sizeof(err) > 0) {
                write("         Reason: %O\n", err[0]);
            } else {
                write("         Reason: %O\n", err);
            }
        } else {
            passed++;
            write("  [PASS] %s\n", case_name);
        }
    }

    void assert_true(mixed val, string|void msg) {
        if (!val) {
            error(msg || "Assertion failed: expected truthy value\n");
        }
    }

    void assert_false(mixed val, string|void msg) {
        if (val) {
            error(msg || "Assertion failed: expected falsy value\n");
        }
    }

    void assert_equal(mixed a, mixed b, string|void msg) {
        if (a != b) {
            error(msg || sprintf("Assertion failed: expected %O but got %O\n", a, b));
        }
    }

    void assert_not_equal(mixed a, mixed b, string|void msg) {
        if (a == b) {
            error(msg || sprintf("Assertion failed: values are unexpectedly equal: %O\n", a));
        }
    }

    void assert_null(mixed val, string|void msg) {
        if (!zero_type(val) && val != 0) {
            error(msg || sprintf("Assertion failed: expected null/zero but got %O\n", val));
        }
    }

    void assert_not_null(mixed val, string|void msg) {
        if (zero_type(val) || val == 0) {
            error(msg || "Assertion failed: expected non-null value\n");
        }
    }

    void assert_throws(function fn, string|void msg) {
        mixed err = catch {
            fn();
        };
        if (!err) {
            error(msg || "Assertion failed: expected exception was not thrown\n");
        }
    }

    int report() {
        int duration = time() - suite_start_time;
        write("--- Results for %s: %d passed, %d failed (took %ds) ---\n",
              suite_name, passed, failed, duration);
        return failed == 0 ? 0 : 1;
    }
}
