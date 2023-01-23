// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Point-in-Time Snapshot Manager and WAL Compactor

string directory;
Thread.Mutex snapshot_mutex;

void create(string _dir) {
    directory = _dir;
    snapshot_mutex = Thread.Mutex();
    if (!Stdio.is_dir(directory)) {
        Stdio.mkdirhier(directory);
    }
}

//! Write an atomic point-in-time snapshot of broker state up to current_lsn.
//! Returns 1 on success, 0 on failure.
int write_snapshot(object broker, int current_lsn) {
    Thread.MutexKey key = snapshot_mutex->lock();

    array(object) retained = broker->retained_store->to_array();
    array(mapping(string:mixed)) retained_serialized = ({});
    foreach (retained, object rm) {
        retained_serialized += ({ ([
            "topic": rm->topic,
            "payload": rm->payload,
            "qos": rm->qos,
            "ts": rm->timestamp
        ]) });
    }

    array(object) sessions = broker->session_manager->to_array();
    array(mapping(string:mixed)) sessions_serialized = ({});
    foreach (sessions, object sess) {
        array(string) subs = indices(sess->subscriptions);
        sessions_serialized += ({ ([
            "client_id": sess->client_id,
            "expiry": sess->session_expiry_interval,
            "subs": subs
        ]) });
    }

    mapping(string:mixed) snap_data = ([
        "version": Binnacle.version(),
        "lsn": current_lsn,
        "timestamp": time(),
        "retained": retained_serialized,
        "sessions": sessions_serialized
    ]);

    string json_str = Standards.JSON.encode(snap_data);
    string crc_str = Binnacle.Utils.Crc32.calculate_binary(json_str);
    string full_payload = sprintf("BSNP%8c", current_lsn) + json_str + crc_str;

    string final_name = sprintf("snapshot_%016x.snap", current_lsn);
    string final_path = Stdio.append_path(directory, final_name);
    string tmp_path = final_path + ".tmp";

    Stdio.File f = Stdio.File();
    if (!f->open(tmp_path, "cwt")) {
        Binnacle.Utils.Logger.error("snap", sprintf("Failed to create snapshot tmp file %s", tmp_path));
        destruct(key);
        return 0;
    }

    f->write(full_payload);
    f->sync();
    f->close();

    // Atomic rename
    mv(tmp_path, final_path);
    Binnacle.Utils.Logger.info("snap", sprintf("Saved atomic snapshot up to LSN %d (%s)", current_lsn, final_name));

    destruct(key);
    return 1;
}

//! Locate and load the most recent snapshot file.
//! Returns ({ int snapshot_lsn, mapping snapshot_data }) or ({ 0, 0 }) if none found.
array(mixed) load_latest_snapshot() {
    Thread.MutexKey key = snapshot_mutex->lock();

    if (!Stdio.is_dir(directory)) {
        destruct(key);
        return ({ 0, 0 });
    }

    array(string) files = get_dir(directory);
    if (!files) {
        destruct(key);
        return ({ 0, 0 });
    }

    array(string) snaps = ({});
    foreach (files, string f) {
        if (has_suffix(f, ".snap")) snaps += ({ f });
    }

    if (sizeof(snaps) == 0) {
        destruct(key);
        return ({ 0, 0 });
    }

    snaps = sort(snaps);
    string latest_name = snaps[-1];
    string latest_path = Stdio.append_path(directory, latest_name);

    string raw = Stdio.read_file(latest_path);
    if (!raw || sizeof(raw) < 16) {
        destruct(key);
        return ({ 0, 0 });
    }

    string magic = raw[0..3];
    if (magic != "BSNP") {
        Binnacle.Utils.Logger.error("snap", sprintf("Invalid snapshot magic in %s", latest_name));
        destruct(key);
        return ({ 0, 0 });
    }

    int lsn;
    sscanf(raw[4..11], "%8c", lsn);
    string json_body = raw[12..sizeof(raw)-5];
    int expected_crc;
    sscanf(raw[sizeof(raw)-4..], "%4c", expected_crc);

    int actual_crc = Binnacle.Utils.Crc32.calculate(json_body);
    if (actual_crc != (expected_crc & 0xFFFFFFFF)) {
        Binnacle.Utils.Logger.error("snap", sprintf("Snapshot CRC32 corruption in %s", latest_name));
        destruct(key);
        return ({ 0, 0 });
    }

    mixed data = 0;
    catch {
        data = Standards.JSON.decode(json_body);
    };

    Binnacle.Utils.Logger.info("snap", sprintf("Loaded valid snapshot from %s (LSN %d)", latest_name, lsn));
    destruct(key);
    return ({ lsn, data });
}
