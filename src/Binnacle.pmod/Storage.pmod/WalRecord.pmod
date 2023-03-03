// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Write-Ahead Log (WAL) Binary Record Codec and Model

constant MAGIC = "BINL";
constant MAGIC_BYTES = 0x42494E4C;

constant TYPE_RETAINED_PUT    = 1;
constant TYPE_RETAINED_DEL    = 2;
constant TYPE_SESSION_UPSERT  = 3;
constant TYPE_SESSION_DELETE  = 4;
constant TYPE_SNAPSHOT_START  = 10;
constant TYPE_SNAPSHOT_END    = 11;

class Record {
    int record_type;
    int sequence_number;
    int timestamp_ns;
    string payload_data;
    int crc32_checksum;

    void create(int _type, int _seq, string _data, int|void _ts) {
        record_type = _type;
        sequence_number = _seq;
        payload_data = _data || "";
        timestamp_ns = zero_type(_ts) ? Binnacle.Utils.Time.monotonic_ns() : _ts;
        crc32_checksum = 0;
    }

    string encode() {
        int data_len = sizeof(payload_data);
        string header = sprintf("%s%c%8c%8c%4c",
                                MAGIC, record_type, sequence_number, timestamp_ns, data_len);
        string raw_record = header + payload_data;
        crc32_checksum = Binnacle.Utils.Crc32.calculate(raw_record);
        string crc_bytes = sprintf("%4c", crc32_checksum);
        return raw_record + crc_bytes;
    }
}

Record create_record(int _type, int _seq, string _data, int|void _ts) {
    return Record(_type, _seq, _data, _ts);
}

//! Decode a WAL record from a binary string buffer starting at offset.
//! Returns ({ object Record, int bytes_consumed }).
array(mixed) decode(string buf, int|void offset) {
    int idx = offset || 0;
    int len = sizeof(buf);

    if ((len - idx) < 29) {
        throw(Binnacle.Errors.StorageError("Truncated WAL record header"));
    }

    string magic = buf[idx..idx+3];
    idx += 4;
    if (magic != MAGIC) {
        throw(Binnacle.Errors.StorageError(sprintf("Corrupted WAL record magic: %s", magic)));
    }

    int r_type = buf[idx++];
    int seq, ts_ns, data_len;
    sscanf(buf[idx..idx+7], "%8c", seq);
    idx += 8;
    sscanf(buf[idx..idx+7], "%8c", ts_ns);
    idx += 8;
    sscanf(buf[idx..idx+3], "%4c", data_len);
    idx += 4;

    int expected_total = 25 + data_len + 4;
    if ((len - (idx - 25)) < expected_total) {
        throw(Binnacle.Errors.StorageError("Truncated WAL record payload"));
    }

    string data = buf[idx..idx+data_len-1];
    idx += data_len;

    int expected_crc;
    sscanf(buf[idx..idx+3], "%4c", expected_crc);
    idx += 4;

    int start_pos = offset || 0;
    string checked_bytes = buf[start_pos .. idx - 5];
    int actual_crc = Binnacle.Utils.Crc32.calculate(checked_bytes);

    if (actual_crc != (expected_crc & 0xFFFFFFFF)) {
        throw(Binnacle.Errors.StorageError(
            sprintf("WAL CRC32 mismatch: actual 0x%08x, expected 0x%08x", actual_crc, expected_crc)
        ));
    }

    object rec = Record(r_type, seq, data, ts_ns);
    rec->crc32_checksum = actual_crc;

    return ({ rec, expected_total });
}
