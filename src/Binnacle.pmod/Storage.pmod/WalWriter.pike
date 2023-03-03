// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Segmented Write-Ahead Log Writer

string directory;
int max_segment_size;
int sync_on_write;
int current_seq;
object current_segment;
Thread.Mutex writer_mutex;

void create(string _dir, int|void _max_size, int|void _sync) {
    directory = _dir;
    max_segment_size = zero_type(_max_size) ? 67108864 : _max_size; // 64 MB
    sync_on_write = zero_type(_sync) ? 0 : _sync;
    current_seq = 1;
    current_segment = 0;
    writer_mutex = Thread.Mutex();

    if (!Stdio.is_dir(directory)) {
        Stdio.mkdirhier(directory);
    }
}

//! Initialize active segment or rollover from existing sequence number.
void init_sequence(int start_seq) {
    Thread.MutexKey key = writer_mutex->lock();
    current_seq = start_seq;
    _open_new_segment(current_seq);
    destruct(key);
}

//! Append a typed WAL record to the log.
//! Returns monotonic sequence number assigned to the record.
int append_record(int record_type, string payload_data) {
    Thread.MutexKey key = writer_mutex->lock();

    if (!current_segment) {
        _open_new_segment(current_seq);
    }

    int seq = current_seq++;
    object rec = .WalRecord.create_record(record_type, seq, payload_data);
    string encoded = rec->encode();

    // Check segment rollover
    if ((current_segment->size() + sizeof(encoded)) > max_segment_size) {
        current_segment->sync();
        current_segment->close();
        _open_new_segment(seq);
    }

    current_segment->append(encoded);
    if (sync_on_write) {
        current_segment->sync();
    }

    destruct(key);
    return seq;
}

private void _open_new_segment(int start_seq) {
    string fname = sprintf("%016x.wal", start_seq);
    string fpath = Stdio.append_path(directory, fname);
    current_segment = .SegmentFile(fpath, start_seq);
    if (!current_segment->open_for_append()) {
        Binnacle.Utils.Logger.error("wal", sprintf("Failed to open WAL segment file %s", fpath));
    } else {
        Binnacle.Utils.Logger.info("wal", sprintf("Opened new WAL segment: %s", fname));
    }
}

//! Flush buffered writes to disk.
void flush() {
    Thread.MutexKey key = writer_mutex->lock();
    if (current_segment) {
        current_segment->sync();
    }
    destruct(key);
}

//! Close writer.
void close() {
    Thread.MutexKey key = writer_mutex->lock();
    if (current_segment) {
        current_segment->sync();
        current_segment->close();
        current_segment = 0;
    }
    destruct(key);
}
