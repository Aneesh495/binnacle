// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Write-Ahead Log Reader and Crash Recovery Scanner

string directory;

void create(string _dir) {
    directory = _dir;
}

//! Scan and replay all WAL segments found in directory.
//! Returns ({ array(object) records, int highest_sequence_number }).
array(mixed) recover_all() {
    if (!Stdio.is_dir(directory)) {
        return ({ ({}), 0 });
    }

    array(string) files = get_dir(directory);
    if (!files) return ({ ({}), 0 });

    array(string) wal_files = ({});
    foreach (files, string f) {
        if (has_suffix(f, ".wal")) {
            wal_files += ({ f });
        }
    }

    wal_files = sort(wal_files);
    array(object) all_records = ({});
    int max_seq = 0;

    foreach (wal_files, string fname) {
        string fpath = Stdio.append_path(directory, fname);
        array(object) seg_records = read_segment_file(fpath);

        foreach (seg_records, object rec) {
            all_records += ({ rec });
            if (rec->sequence_number > max_seq) {
                max_seq = rec->sequence_number;
            }
        }
    }

    Binnacle.Utils.Logger.info("wal", sprintf("Recovered %d WAL records across %d segment files (highest LSN: %d)",
                               sizeof(all_records), sizeof(wal_files), max_seq));

    return ({ all_records, max_seq });
}

//! Read and validate all records from an individual segment file.
array(object) read_segment_file(string path) {
    array(object) records = ({});
    string content = Stdio.read_file(path);
    if (!content || sizeof(content) == 0) return records;

    int idx = 0;
    int len = sizeof(content);

    while (idx < len) {
        if ((len - idx) < 29) {
            // Incomplete header at end of file (likely torn write during crash)
            break;
        }

        array(mixed)|int dec_res = 0;
        catch {
            dec_res = .WalRecord.decode(content, idx);
        };

        if (!dec_res) {
            Binnacle.Utils.Logger.warn("wal", sprintf("Torn record encountered at offset %d in %s, ending segment scan", idx, path));
            break;
        }

        object rec = dec_res[0];
        int consumed = dec_res[1];
        records += ({ rec });
        idx += consumed;
    }

    return records;
}
