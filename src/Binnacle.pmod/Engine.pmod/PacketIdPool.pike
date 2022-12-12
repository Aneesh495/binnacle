// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// 16-Bit Packet Identifier Recycling Pool (OASIS MQTT 5.0 Section 2.2.1)

int next_id;
multiset(int) in_use;
Thread.Mutex pool_mutex;

void create() {
    next_id = 1;
    in_use = (<>);
    pool_mutex = Thread.Mutex();
}

//! Acquire the next available 16-bit packet identifier (1..65535).
//! Throws QuotaExceededError if all 65,535 packet IDs are currently in flight.
int acquire() {
    Thread.MutexKey key = pool_mutex->lock();

    int start = next_id;
    int max_val = Binnacle.Constants.MAX_PACKET_ID;

    while (1) {
        int candidate = next_id++;
        if (next_id > max_val) {
            next_id = 1; // Wrap around to 1 (0 is forbidden)
        }

        if (!in_use[candidate]) {
            in_use[candidate] = 1;
            destruct(key);
            return candidate;
        }

        if (next_id == start) {
            destruct(key);
            throw(Binnacle.Errors.QuotaExceededError(
                "All 65,535 packet identifiers are currently exhausted",
                Binnacle.Constants.RC_QUOTA_EXCEEDED
            ));
        }
    }
}

//! Release a completed packet identifier back to the recycling pool.
void release(int pid) {
    if (pid <= 0 || pid > Binnacle.Constants.MAX_PACKET_ID) return;

    Thread.MutexKey key = pool_mutex->lock();
    if (in_use[pid]) {
        in_use[pid] = 0;
    }
    destruct(key);
}

//! Check if a packet identifier is currently active.
int is_in_use(int pid) {
    Thread.MutexKey key = pool_mutex->lock();
    int active = in_use[pid] ? 1 : 0;
    destruct(key);
    return active;
}

//! Return number of actively leased packet identifiers.
int active_count() {
    Thread.MutexKey key = pool_mutex->lock();
    int count = sizeof(in_use);
    destruct(key);
    return count;
}
