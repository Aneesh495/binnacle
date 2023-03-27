// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/unit/test_rule_engine.pike
// Description: Unit tests for JSONPath extraction, SQL-like predicates, and streaming window aggregations.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Stream Processing and Complex Event Rules Engine");

    runner->test("JSONPath nested dot and array index extraction", lambda() {
        mapping data = ([
            "vehicle": ([
                "vin": "1HGCR2F80001",
                "sensors": ({
                    ([ "type": "temp", "val": 88.5 ]),
                    ([ "type": "pressure", "val": 32.1 ])
                }),
                "engine": ([
                    "rpm": 3200
                ])
            ])
        ]);

        runner->assert_equal(Binnacle.Stream.JsonPath.get_value(data, "vehicle.vin"), "1HGCR2F80001");
        runner->assert_equal(Binnacle.Stream.JsonPath.get_value(data, "vehicle.engine.rpm"), 3200);
        runner->assert_equal(Binnacle.Stream.JsonPath.get_value(data, "vehicle.sensors[0].val"), 88.5);
        runner->assert_equal(Binnacle.Stream.JsonPath.get_value(data, "vehicle.sensors[1].type"), "pressure");
        runner->assert_null(Binnacle.Stream.JsonPath.get_value(data, "vehicle.sensors[99].val"));
    });

    runner->test("SQL-like predicate expression evaluation", lambda() {
        object expr1 = Binnacle.Stream.ExpressionParser("temperature > 50");
        runner->assert_true(expr1->evaluate(([ "temperature": 75 ])));
        runner->assert_false(expr1->evaluate(([ "temperature": 30 ])));

        object expr2 = Binnacle.Stream.ExpressionParser("status == 'CRITICAL' AND speed >= 100");
        runner->assert_true(expr2->evaluate(([ "status": "CRITICAL", "speed": 120 ])));
        runner->assert_false(expr2->evaluate(([ "status": "OK", "speed": 120 ])));
        runner->assert_false(expr2->evaluate(([ "status": "CRITICAL", "speed": 80 ])));

        object expr3 = Binnacle.Stream.ExpressionParser("topic CONTAINS alert");
        runner->assert_true(expr3->evaluate(([ "topic": "fleet/vehicle01/alert" ])));
        runner->assert_false(expr3->evaluate(([ "topic": "fleet/vehicle01/telemetry" ])));
    });

    runner->test("WindowAggregator statistics computation", lambda() {
        object win = Binnacle.Stream.WindowAggregator("sliding", 60);
        win->add_sample(10.0, time());
        win->add_sample(20.0, time());
        win->add_sample(30.0, time());

        mapping metrics = win->compute_metrics();
        runner->assert_equal(metrics->count, 3.0);
        runner->assert_equal(metrics->min, 10.0);
        runner->assert_equal(metrics->max, 30.0);
        runner->assert_equal(metrics->avg, 20.0);
        runner->assert_equal(metrics->sum, 60.0);
    });

    return runner->report();
}
