/*
 * Binnacle: Distributed MQTT 5.0 Telemetry Broker and Message Fabric
 * Multi-Threaded Low-Latency C Benchmark Harness
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "hdr_histogram.h"

#define DEFAULT_PORT 1883
#define DEFAULT_THREADS 4
#define DEFAULT_MESSAGES_PER_THREAD 25000
#define PAYLOAD_SIZE 64

typedef struct {
    int thread_id;
    const char* host;
    int port;
    int message_count;
    hdr_histogram_t histogram;
    uint64_t bytes_sent;
    uint64_t messages_sent;
} worker_arg_t;

static inline uint64_t get_nanoseconds(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int connect_tcp(const char* host, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
        close(sock);
        return -1;
    }

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }

    return sock;
}

static int send_connect(int sock, const char* client_id) {
    /* Minimal MQTT 5.0 CONNECT packet */
    uint8_t pkt[128];
    int idx = 0;

    pkt[idx++] = 0x10; /* CONNECT */
    int len_pos = idx++;

    /* Protocol name "MQTT" */
    pkt[idx++] = 0x00; pkt[idx++] = 0x04;
    pkt[idx++] = 'M'; pkt[idx++] = 'Q'; pkt[idx++] = 'T'; pkt[idx++] = 'T';
    pkt[idx++] = 0x05; /* Level 5.0 */
    pkt[idx++] = 0x02; /* CleanStart = 1 */
    pkt[idx++] = 0x00; pkt[idx++] = 0x3C; /* Keepalive 60s */
    pkt[idx++] = 0x00; /* Property Length 0 */

    /* Client ID */
    int cid_len = strlen(client_id);
    pkt[idx++] = (cid_len >> 8) & 0xFF;
    pkt[idx++] = cid_len & 0xFF;
    memcpy(&pkt[idx], client_id, cid_len);
    idx += cid_len;

    pkt[len_pos] = idx - 2;

    if (send(sock, pkt, idx, 0) != idx) return -1;

    /* Await CONNACK */
    uint8_t ack[32];
    int r = recv(sock, ack, sizeof(ack), 0);
    if (r < 4 || (ack[0] >> 4) != 2) return -1;

    return 0;
}

static void* worker_thread(void* arg) {
    worker_arg_t* w = (worker_arg_t*)arg;
    char cid[64];
    snprintf(cid, sizeof(cid), "bench-c-%d-%d", w->thread_id, (int)time(NULL));

    int sock = connect_tcp(w->host, w->port);
    if (sock < 0) {
        fprintf(stderr, "Thread %d: Failed to connect\n", w->thread_id);
        return NULL;
    }

    if (send_connect(sock, cid) < 0) {
        fprintf(stderr, "Thread %d: Failed CONNECT handshake\n", w->thread_id);
        close(sock);
        return NULL;
    }

    char topic[64];
    snprintf(topic, sizeof(topic), "telemetry/bench/sensor-%d", w->thread_id);
    int top_len = strlen(topic);

    char payload[PAYLOAD_SIZE];
    memset(payload, 'A', sizeof(payload));

    /* Pre-assemble QoS 0 PUBLISH packet buffer */
    uint8_t pub_buf[256];
    int p_idx = 0;
    pub_buf[p_idx++] = 0x30; /* PUBLISH QoS 0 */

    int var_len = 2 + top_len + 1 + PAYLOAD_SIZE; /* Topic + prop_len(0) + payload */
    pub_buf[p_idx++] = var_len;
    pub_buf[p_idx++] = (top_len >> 8) & 0xFF;
    pub_buf[p_idx++] = top_len & 0xFF;
    memcpy(&pub_buf[p_idx], topic, top_len);
    p_idx += top_len;
    pub_buf[p_idx++] = 0x00; /* Property Length 0 */
    memcpy(&pub_buf[p_idx], payload, PAYLOAD_SIZE);
    p_idx += PAYLOAD_SIZE;

    for (int i = 0; i < w->message_count; i++) {
        uint64_t t0 = get_nanoseconds();
        ssize_t sent = send(sock, pub_buf, p_idx, 0);
        uint64_t t1 = get_nanoseconds();

        if (sent == p_idx) {
            w->messages_sent++;
            w->bytes_sent += sent;
            hdr_record(&w->histogram, (t1 - t0) / 1000ULL); /* Microseconds */
        }
    }

    close(sock);
    return NULL;
}

int main(int argc, char** argv) {
    const char* host = "127.0.0.1";
    int port = DEFAULT_PORT;
    int threads = DEFAULT_THREADS;
    int messages = DEFAULT_MESSAGES_PER_THREAD;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-h") == 0 && i + 1 < argc) host = argv[++i];
        else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) port = atoi(argv[++i]);
        else if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) threads = atoi(argv[++i]);
        else if (strcmp(argv[i], "-n") == 0 && i + 1 < argc) messages = atoi(argv[++i]);
    }

    printf("====================================================================\n");
    printf(" Binnacle High-Throughput C Benchmark Harness\n");
    printf(" Target       : %s:%d\n", host, port);
    printf(" Concurrency  : %d threads\n", threads);
    printf(" Messages/Thr : %d (Total: %d messages)\n", messages, threads * messages);
    printf(" Payload Size : %d bytes\n", PAYLOAD_SIZE);
    printf("====================================================================\n");

    pthread_t thread_ids[64];
    worker_arg_t args[64];

    uint64_t start_time = get_nanoseconds();

    for (int i = 0; i < threads && i < 64; i++) {
        args[i].thread_id = i;
        args[i].host = host;
        args[i].port = port;
        args[i].message_count = messages;
        args[i].bytes_sent = 0;
        args[i].messages_sent = 0;
        hdr_init(&args[i].histogram);
        pthread_create(&thread_ids[i], NULL, worker_thread, &args[i]);
    }

    hdr_histogram_t aggregate;
    hdr_init(&aggregate);
    uint64_t total_sent = 0;
    uint64_t total_bytes = 0;

    for (int i = 0; i < threads && i < 64; i++) {
        pthread_join(thread_ids[i], NULL);
        total_sent += args[i].messages_sent;
        total_bytes += args[i].bytes_sent;
        aggregate.total_count += args[i].histogram.total_count;
        aggregate.sum_values += args[i].histogram.sum_values;
        if (args[i].histogram.min_value < aggregate.min_value)
            aggregate.min_value = args[i].histogram.min_value;
        if (args[i].histogram.max_value > aggregate.max_value)
            aggregate.max_value = args[i].histogram.max_value;

        for (int j = 0; j < 1024; j++) {
            aggregate.counts[j] += args[i].histogram.counts[j];
        }
    }

    uint64_t end_time = get_nanoseconds();
    double elapsed_sec = (double)(end_time - start_time) / 1000000000.0;
    double throughput_mps = (double)total_sent / elapsed_sec;
    double throughput_mb = ((double)total_bytes / (1024.0 * 1024.0)) / elapsed_sec;

    printf("\nBenchmark Completed in %.3f seconds:\n", elapsed_sec);
    printf("  Throughput   : %.2f msg/sec (%.2f MB/sec)\n", throughput_mps, throughput_mb);
    printf("  Total Sent   : %llu messages (%llu bytes)\n", total_sent, total_bytes);
    printf("  Latency p50  : %llu us\n", hdr_value_at_percentile(&aggregate, 50.0));
    printf("  Latency p90  : %llu us\n", hdr_value_at_percentile(&aggregate, 90.0));
    printf("  Latency p95  : %llu us\n", hdr_value_at_percentile(&aggregate, 95.0));
    printf("  Latency p99  : %llu us\n", hdr_value_at_percentile(&aggregate, 99.0));
    printf("  Latency Mean : %.2f us\n", hdr_mean(&aggregate));
    printf("  Latency Min  : %llu us\n", aggregate.min_value);
    printf("  Latency Max  : %llu us\n", aggregate.max_value);
    printf("====================================================================\n");

    return 0;
}
