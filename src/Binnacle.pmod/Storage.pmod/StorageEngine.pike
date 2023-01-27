// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Integrated Storage Engine Coordinating WAL, Crash Recovery, and Snapshots

object broker;
string data_directory;
object writer;
object reader;
object snapshot_manager;
int highest_lsn;
int compaction_interval_seconds;
mixed compaction_callout_id;
Thread.Mutex engine_mutex;

void create(object _broker, string _dir, int|void _compaction_interval) {
    broker = _broker;
    data_directory = _dir;
    compaction_interval_seconds = zero_type(_compaction_interval) ? 300 : _compaction_interval;
    highest_lsn = 0;
    compaction_callout_id = 0;
    engine_mutex = Thread.Mutex();

    writer = .WalWriter(data_directory);
    reader = .WalReader(data_directory);
    snapshot_manager = .SnapshotManager(data_directory);
}

//! Initialize storage engine, execute crash recovery replay, and resume write logging.
void start() {
    Thread.MutexKey key = engine_mutex->lock();

    // 1. Recover latest snapshot if available
    array(mixed) snap_res = snapshot_manager->load_latest_snapshot();
    int snap_lsn = snap_res[0];
    mapping snap_data = snap_res[1];

    if (snap_data) {
        _restore_snapshot(snap_data);
        highest_lsn = snap_lsn;
    }

    // 2. Replay subsequent WAL records
    array(mixed) rec_res = reader->recover_all();
    array(object) records = rec_res[0];
    int max_wal_seq = rec_res[1];

    foreach (records, object rec) {
        if (rec->sequence_number > highest_lsn) {
            _replay_record(rec);
            highest_lsn = rec->sequence_number;
        }
    }

    // 3. Initialize writer with next sequence number
    writer->init_sequence(highest_lsn + 1);
    destruct(key);

    Binnacle.Utils.Logger.info("storage", sprintf("Storage engine started at LSN %d", highest_lsn));
    _schedule_compaction();
}

//! Stop storage engine and flush pending WAL writes.
void stop() {
    if (compaction_callout_id) {
        remove_call_out(compaction_callout_id);
        compaction_callout_id = 0;
    }
    writer->flush();
    writer->close();
}

private void _schedule_compaction() {
    if (compaction_interval_seconds > 0) {
        compaction_callout_id = call_out(_on_compaction_tick, compaction_interval_seconds);
    }
}

private void _on_compaction_tick() {
    snapshot_manager->write_snapshot(broker, highest_lsn);
    _schedule_compaction();
}

//! Record a retained message write into WAL.
void append_retained(string topic, string payload, int qos, mapping(int:mixed)|void props) {
    mapping(string:mixed) data = ([
        "topic": topic,
        "payload": payload,
        "qos": qos
    ]);
    string json_str = Standards.JSON.encode(data);
    int type = (sizeof(payload) > 0) ? .WalRecord.TYPE_RETAINED_PUT : .WalRecord.TYPE_RETAINED_DEL;
    highest_lsn = writer->append_record(type, json_str);
}

//! Replay an individual WAL record during crash recovery.
private void _replay_record(object rec) {
    mixed parsed = 0;
    catch {
        parsed = Standards.JSON.decode(rec->payload_data);
    };
    if (!parsed) return;

    switch (rec->record_type) {
        case .WalRecord.TYPE_RETAINED_PUT:
            broker->retained_store->put(parsed->topic, parsed->payload, parsed->qos);
            break;

        case .WalRecord.TYPE_RETAINED_DEL:
            broker->retained_store->delete_topic(parsed->topic);
            break;

        default:
            break;
    }
}

//! Restore retained messages and sessions from snapshot data.
private void _restore_snapshot(mapping data) {
    if (has_index(data, "retained") && arrayp(data->retained)) {
        foreach (data->retained, mapping rm) {
            broker->retained_store->put(rm->topic, rm->payload, rm->qos);
        }
    }
}
