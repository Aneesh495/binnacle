// Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
// Bounded Offline Message Queue and Eviction Manager

int max_capacity;
string drop_strategy; // "drop_oldest" or "drop_newest"
array(object) queue;
Thread.Mutex queue_mutex;

void create(int|void _max, string|void _strategy) {
    max_capacity = zero_type(_max) ? Binnacle.Constants.DEFAULT_MAX_OFFLINE_MESSAGES : _max;
    drop_strategy = _strategy || "drop_oldest";
    queue = ({});
    queue_mutex = Thread.Mutex();
}

//! Enqueue a message for an offline client.
//! Applies configured eviction policy if capacity is reached.
int enqueue(object pub_pkt) {
    Thread.MutexKey key = queue_mutex->lock();

    if (sizeof(queue) >= max_capacity) {
        if (drop_strategy == "drop_oldest") {
            // Drop head of queue
            queue = queue[1..];
        } else {
            // Drop newest (reject current message)
            destruct(key);
            return 0;
        }
    }

    queue += ({ pub_pkt });
    destruct(key);
    return 1;
}

//! Dequeue the next available message.
object dequeue() {
    Thread.MutexKey key = queue_mutex->lock();
    if (sizeof(queue) == 0) {
        destruct(key);
        return 0;
    }
    object msg = queue[0];
    queue = queue[1..];
    destruct(key);
    return msg;
}

//! Return current queue size.
int size() {
    Thread.MutexKey key = queue_mutex->lock();
    int s = sizeof(queue);
    destruct(key);
    return s;
}

//! Check if queue is empty.
int is_empty() {
    Thread.MutexKey key = queue_mutex->lock();
    int empty = (sizeof(queue) == 0);
    destruct(key);
    return empty;
}

//! Drain and clear all messages from queue.
array(object) drain() {
    Thread.MutexKey key = queue_mutex->lock();
    array(object) all = queue;
    queue = ({});
    destruct(key);
    return all;
}

void clear() {
    Thread.MutexKey key = queue_mutex->lock();
    queue = ({});
    destruct(key);
}
