// Binnacle: Distributed MQTT 5.0 Telemetry Broker
// File: tests/unit/test_wal_storage.pike
// Description: Unit tests for Write-Ahead Log serialization, checksums, and crash recovery.

int main() {
    object runner = TestFramework.TestRunner();
    runner->start_suite("Write-Ahead Log (WAL) Storage Engine");

    runner->test("WalRecord binary encoding and decoding round-trip", lambda() {
        object rec = Binnacle.Storage.WalRecord.create_record(
            Binnacle.Storage.WalRecord.TYPE_RETAINED_PUT, 42, "payload-content-123", 1000000
        );
        string encoded = rec->encode();
        runner->assert_true(sizeof(encoded) > 29, "Encoded record must be larger than header");

        array res = Binnacle.Storage.WalRecord.decode(encoded, 0);
        runner->assert_not_null(res);
        object decoded = res[0];
        runner->assert_equal(decoded->record_type, Binnacle.Storage.WalRecord.TYPE_RETAINED_PUT);
        runner->assert_equal(decoded->sequence_number, 42);
        runner->assert_equal(decoded->payload_data, "payload-content-123");
        runner->assert_equal(decoded->timestamp_ns, 1000000);
    });

    runner->test("WalRecord detects corrupted payload CRC", lambda() {
        object rec = Binnacle.Storage.WalRecord.create_record(
            Binnacle.Storage.WalRecord.TYPE_RETAINED_PUT, 1, "valid-payload"
        );
        string encoded = rec->encode();

        // Corrupt one payload byte
        string corrupted = encoded;
        corrupted[26] = corrupted[26] ^ 0xFF;

        runner->assert_throws(lambda() {
            Binnacle.Storage.WalRecord.decode(corrupted, 0);
        }, "Corrupted payload CRC must throw DataCorruptionError");
    });

    runner->test("WalWriter and WalReader recovery lifecycle", lambda() {
        string test_dir = "/tmp/binnacle_test_wal_" + time();
        object writer = Binnacle.Storage.WalWriter(test_dir, 1024 * 1024, 1);

        int seq1 = writer->append_record(Binnacle.Storage.WalRecord.TYPE_RETAINED_PUT, "record-A");
        int seq2 = writer->append_record(Binnacle.Storage.WalRecord.TYPE_RETAINED_PUT, "record-B");
        int seq3 = writer->append_record(Binnacle.Storage.WalRecord.TYPE_SESSION_UPSERT, "session-C");

        runner->assert_equal(seq1, 1);
        runner->assert_equal(seq2, 2);
        runner->assert_equal(seq3, 3);

        writer->close();

        // Recover from directory using WalReader
        object reader = Binnacle.Storage.WalReader(test_dir);
        array recov = reader->recover_all();
        array records = recov[0];
        int max_seq = recov[1];

        runner->assert_equal(sizeof(records), 3);
        runner->assert_equal(max_seq, 3);
        runner->assert_equal(records[0]->payload_data, "record-A");
        runner->assert_equal(records[1]->payload_data, "record-B");
        runner->assert_equal(records[2]->payload_data, "session-C");

        // Clean up test files
        Process.run(({ "rm", "-rf", test_dir }));
    });

    return runner->report();
}
