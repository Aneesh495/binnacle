// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Append-Only Storage Segment File Handler

string file_path;
int start_sequence;
int current_bytes;
Stdio.File file_handle;
Thread.Mutex file_mutex;

void create(string _path, int _start_seq) {
    file_path = _path;
    start_sequence = _start_seq;
    current_bytes = 0;
    file_handle = 0;
    file_mutex = Thread.Mutex();
}

//! Open the segment file for appending (creates file if not present).
int open_for_append() {
    Thread.MutexKey key = file_mutex->lock();
    file_handle = Stdio.File();
    if (!file_handle->open(file_path, "cwa")) {
        file_handle = 0;
        destruct(key);
        return 0;
    }
    // Seek to end to determine existing size
    current_bytes = file_handle->seek(0, Stdio.SEEK_END);
    destruct(key);
    return 1;
}

//! Append raw encoded bytes to the segment file.
int append(string bytes) {
    Thread.MutexKey key = file_mutex->lock();
    if (!file_handle) {
        destruct(key);
        return 0;
    }

    int written = file_handle->write(bytes);
    if (written > 0) {
        current_bytes += written;
    }
    destruct(key);
    return written == sizeof(bytes);
}

//! Force disk sync (fsync) to ensure hardware durability.
void sync() {
    Thread.MutexKey key = file_mutex->lock();
    if (file_handle) {
        file_handle->sync();
    }
    destruct(key);
}

//! Close the segment file handle.
void close() {
    Thread.MutexKey key = file_mutex->lock();
    if (file_handle) {
        file_handle->close();
        file_handle = 0;
    }
    destruct(key);
}

int size() {
    return current_bytes;
}
