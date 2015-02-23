#include <assert.h>
#include <stdint.h>
#include <inttypes.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>

#include <pthread.h>

#define MAX_TARGETS 10
#define TARGET_BUFFER_SIZE (1*1024*1024)
#define TARGET_SEND_THRESHOLD (256*1024)

#if 0
#define FILE_TRANSFER_BUFFER_SIZE (1*1024*1024)

#define MAX_CHUNKS_PER_FILE 10
typedef int64_t FileID;
typedef struct {
    FileID file_id;
    uint64_t full_file_size;
    int16_t sources[MAX_CHUNKS_PER_FILE];
    int16_t P_rank;
    int16_t Q_rank;
} Task;
#endif

static int next_chr(const char *p, size_t len, char c)
{
    for (size_t i = 0; i < len; i++)
        if (p[i] == c)
            return i;
    return 0;
}
static int next_zero(const char *p, size_t len)
{
    return next_chr(p, len, '\0');
}

static unsigned simple_hash(const char *p, int len)
{
    unsigned h = 5381;
    for (int i = 0; i < len; i++)
        h = h + (h << 5) + p[i];
    return h;
}

typedef struct {
    uint64_t byte_size;
    uint64_t timestamp;
    uint64_t path_len;
    char path[];
} packed_file_info;

static uint8_t dst_buffer[MAX_TARGETS][TARGET_BUFFER_SIZE];
static size_t  dst_written[MAX_TARGETS] = {0};
static size_t  dst_in_transit[MAX_TARGETS] = {0};

static
void push_to_target(int target, const char *path, int path_len, uint64_t byte_size, uint64_t timestamp)
{
    /* if done sending, shift non-sent data down to 0 */
    /* append new data */
    /* if amount of data > threshold, begin async send to target */
    assert(0 <= target && target < MAX_TARGETS);
    assert(path != NULL);
    assert(path_len > 0);

    size_t in_transit = dst_in_transit[target];
    size_t written = dst_written[target];
    assert(in_transit <= written);
    assert(written <= TARGET_BUFFER_SIZE);
    int can_send = (in_transit == 0);
    uint8_t *buf = dst_buffer[target];
    size_t new_size = written + sizeof(packed_file_info) + path_len;
    if (new_size >= TARGET_BUFFER_SIZE
            /* || is_done_with_prev_async_send(..) */) {
        /* finish_prev_async_send(); */
        size_t sent = in_transit;
        if (sent < written)
            memmove(buf, buf + sent, written - sent);
        written -= sent;
        new_size -= sent;
        dst_in_transit[target] = 0;
    }

    packed_file_info finfo = {byte_size, timestamp, path_len};
    uint8_t *dst = dst_buffer[target] + written;
    memcpy(dst, &finfo, sizeof(packed_file_info));
    memcpy(dst + sizeof(packed_file_info), path, path_len);
    dst_written[target] = written = new_size;
    assert(dst_written[target] == written);

    if (can_send && written >= TARGET_SEND_THRESHOLD) {
        /* begin_async_send(..) */
        dst_in_transit[target] = written;
    }
}

static
void send_remaining_data_to_targets(void)
{
    for (int i = 0; i < MAX_TARGETS; i++)
    {
        /* finish_prev_async_send() */
        /* begin_async_send(..) */
    }
    for (int i = 0; i < MAX_TARGETS; i++)
    {
        /* finish_prev_async_send() */
    }
}

static
void feed_targets_with(FILE *input_file)
{
    size_t buf_size = 64*1024;
    size_t buf_offset = 0;
    char *buf = calloc(1, buf_size);
    int read;
    while ((read = fread(buf + buf_offset, 1, buf_size - buf_offset, input_file)) != 0)
    {
        size_t buf_alive = buf_offset + read;
        const char *bufp = buf;
        for (;;) {
            int len_of_timestamp = next_zero(bufp, buf_alive);
            int len_of_path = next_zero(bufp + len_of_timestamp + 1, buf_alive - len_of_timestamp - 1);
            int len_of_size = next_zero(bufp + len_of_timestamp + len_of_path + 2, buf_alive - len_of_timestamp - len_of_path - 2);
            if (len_of_timestamp == 0 || len_of_path == 0 || len_of_size == 0)
            {
                buf_offset = buf_alive;
                memmove(buf, bufp, buf_alive);
                break;
            }
            const char *timestamp = bufp;
            len_of_timestamp -= 1;
            const char *path = bufp + len_of_timestamp + 2;
            const char *size = path + len_of_path + 1;
            bufp += len_of_timestamp + len_of_path + len_of_size + 4;
            buf_alive -= len_of_timestamp + len_of_path + len_of_size + 4;
            char *tmp;
            uint64_t timestamp_secs = strtoull(timestamp, &tmp, 10);
            uint64_t timestamp_msecs = strtoull(tmp + 1, &tmp, 10);
            (void) timestamp_msecs; /* we ignore fractional part since beegfs doesn't use it */
            uint64_t byte_size = strtoull(size, &tmp, 10);
            unsigned st = (simple_hash(path, len_of_path)) % MAX_TARGETS;
            printf("%u - %" PRIu64 " %*s %" PRIu64 "\n", st, timestamp_secs, len_of_path, path, byte_size);
            push_to_target(st, path, len_of_path, byte_size, timestamp_secs);
        }
    }
    send_remaining_data_to_targets();
    /* tell global-coordinator that we are done */
}

static
void* feed_func(void *state)
{
    (void) state;
    feed_targets_with(stdin);
    return NULL;
}

static
void* eat_func(void *state)
{
    (void) state;

    /*
     * while true:
     *   receive from any
     *   if sender == global-coordinator:
     *     break
     *   parse and add data
     * */
    return NULL;
}

int main(int argc, char **argv)
{
    /* if I am global-coordinator:
     *   do something else
     * else:
     * */
    pthread_t feeder;
    pthread_t eater;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&feeder, &attr, feed_func, NULL);
    pthread_create(&eater, &attr, eat_func, NULL);
    pthread_attr_destroy(&attr);

    void *status_a = NULL;
    void *status_b = NULL;
    int rc = pthread_join(feeder, &status_a) | pthread_join(eater, &status_b);
    if (rc) {
        fprintf(stderr, "ERROR: pthread_join() = %d\n", rc);
        return -1;
    }
    if (status_a != NULL || status_b != NULL) {
        fprintf(stderr, "ERROR: bad thread status?\n");
        return -1;
    }

    /*
     * We have now sent info on all out chunks, and received info on all chunks
     * that we are responsible for.
     * */

    return 0;
}
