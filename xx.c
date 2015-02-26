#include <assert.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <mpi.h>

#define MAX_TARGETS 2
#define TARGET_BUFFER_SIZE (1*1024*1024)
#define TARGET_SEND_THRESHOLD (256*1024)

static const int global_coordinator = 0;
static int mpi_rank;
static int mpi_world_size;

static
void send_sync_message_to(int16_t recieving_rank, int msg_size, const uint8_t msg[static msg_size])
{
    MPI_Ssend((void*)msg, msg_size, MPI_BYTE, recieving_rank, 0, MPI_COMM_WORLD);
}

static
int next_chr(const char *p, ssize_t len, char c)
{
    for (ssize_t i = 0; i < len; i++)
        if (p[i] == c)
            return i;
    return 0;
}
static
int next_zero(const char *p, ssize_t len)
{
    return next_chr(p, len, '\0');
}

static
unsigned simple_hash(const char *p, int len)
{
    unsigned h = 5381;
    for (int i = 0; i < len; i++)
        h = h + (h << 5) + p[i];
    return h;
}

static
int eater_rank_from_st(int storage_target)
{
    return storage_target + 1 + MAX_TARGETS;
}

typedef struct {
    uint64_t byte_size;
    uint64_t timestamp;
    uint64_t path_len;
    char path[];
} packed_file_info;

static ssize_t  dst_written[MAX_TARGETS] = {0};
static ssize_t  dst_in_transit[MAX_TARGETS] = {0};
static MPI_Request async_send_req[MAX_TARGETS] = {0};
static uint8_t dst_buffer[MAX_TARGETS][TARGET_BUFFER_SIZE];

static
int is_done_with_prev_async_send(int target)
{
    if (dst_in_transit[target] == 0)
        return 0;
    MPI_Status stat;
    int flag;
    MPI_Test(&async_send_req[target], &flag, &stat);
    return flag;
}

static
void finish_prev_async_send(int target)
{
    MPI_Status stat;
    MPI_Wait(&async_send_req[target], &stat);

    ssize_t sent = dst_in_transit[target];
    ssize_t written = dst_written[target];
    uint8_t *buf = dst_buffer[target];
    if (sent < written)
        memmove(buf, buf + sent, written - sent);
    dst_written[target] -= sent;
    dst_in_transit[target] = 0;
}

static
void begin_async_send(int target)
{
    assert(dst_in_transit[target] == 0);
    assert(dst_written[target] > 0);
    dst_in_transit[target] = dst_written[target];
    MPI_Isend(
            dst_buffer[target],
            dst_in_transit[target],
            MPI_BYTE,
            eater_rank_from_st(target),
            0,
            MPI_COMM_WORLD,
            &async_send_req[target]);
}

static
void push_to_target(int target, const char *path, int path_len, uint64_t byte_size, uint64_t timestamp)
{
    assert(0 <= target && target < MAX_TARGETS);
    assert(path != NULL);
    assert(path_len > 0);

    ssize_t in_transit = dst_in_transit[target];
    ssize_t written = dst_written[target];
    assert(in_transit <= written);
    assert(written <= TARGET_BUFFER_SIZE);
    ssize_t new_size = written + sizeof(packed_file_info) + path_len;
    if (new_size >= TARGET_BUFFER_SIZE
            || is_done_with_prev_async_send(target)) {
        new_size -= in_transit;
        written -= in_transit;
        in_transit = 0;
        finish_prev_async_send(target);
    }

    packed_file_info finfo = {byte_size, timestamp, path_len};
    uint8_t *dst = dst_buffer[target] + written;
    memcpy(dst, &finfo, sizeof(packed_file_info));
    memcpy(dst + sizeof(packed_file_info), path, path_len);
    dst_written[target] = written = new_size;

    if (in_transit == 0 && written >= TARGET_SEND_THRESHOLD) {
        begin_async_send(target);
    }
}

static
void send_remaining_data_to_targets(void)
{
    for (int i = 0; i < MAX_TARGETS; i++)
    {
        if (dst_in_transit[i] > 0)
            finish_prev_async_send(i);
        if (dst_written[i] > 0)
            begin_async_send(i);
    }
    for (int i = 0; i < MAX_TARGETS; i++)
    {
        if (dst_in_transit[i] > 0)
            finish_prev_async_send(i);
    }
}

static
void feed_targets_with(FILE *input_file)
{
    ssize_t buf_size = 64*1024;
    ssize_t buf_offset = 0;
    char *buf = calloc(1, buf_size);
    int read;
    while ((read = fread(buf + buf_offset, 1, buf_size - buf_offset, input_file)) != 0)
    {
        ssize_t buf_alive = buf_offset + read;
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
            // printf("%u - %" PRIu64 " %*s %" PRIu64 "\n", st, timestamp_secs, len_of_path, path, byte_size);
            push_to_target(
                    st,
                    path,
                    len_of_path,
                    byte_size,
                    timestamp_secs);
        }
    }
    send_remaining_data_to_targets();
    /* tell global-coordinator that we are done */
    uint8_t failed = 0;
    send_sync_message_to(global_coordinator, sizeof(failed), &failed);
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_world_size);

    int feeder_ranks[] = {1,2};
    int eater_ranks[] = {3,4};

    /* 
     * When the feeders are done they have nothing else to do.
     * Broadcasts would still transfer data to them, so we create a group
     * without the feeders.
     *
     * Ideally we could have made the feeders/eaters as two different threads,
     * simply closing the feeder threads when done. But that doesn't work with
     * the MPI version I am testing on - it causes data races and maybe some
     * deadlocks.
     * */
    MPI_Group everyone, not_everyone;
    MPI_Comm_group(MPI_COMM_WORLD, &everyone);
    MPI_Group_excl(everyone, 2, feeder_ranks, &not_everyone);
    MPI_Comm comm;
    MPI_Comm_create(MPI_COMM_WORLD, not_everyone, &comm);

    /*
     * In phase 1 we have 3 kinds of processes:
     *  - global coordinator
     *  - feeders
     *  - eaters
     *
     * An eater simply receives data from anyone (storing it for later) - only
     * stopping when the global coordinator sends them a message.
     *
     * The feeders run through their files/chunks, selects a "random" eater and
     * sends filename, size, etc to it. Once they are done with their files
     * they message the global coordinator.
     *
     * Finally the global coordinator waits until every feeder has told it that
     * they are done processing - then it tells the eaters.
     */
    int first_feeder_rank = feeder_ranks[0];
    int first_eater_rank = eater_ranks[0];
    if (mpi_rank == global_coordinator)
    {
        MPI_Barrier(MPI_COMM_WORLD);
        int still_in_stage_1 = MAX_TARGETS;
        int failed = 0;
        while (still_in_stage_1 > 0) {
            MPI_Status stat;
            int8_t error;
            MPI_Recv(&error, sizeof(error), MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &stat);
            still_in_stage_1 -= 1;
            failed += error;
            printf("gco: got message from %d, with error = %d\n", stat.MPI_SOURCE, error);
        }
        /* Inform all eaters that there is no more food */
        uint8_t dummy = 1;
        for (int i = first_eater_rank; i < mpi_world_size; i++)
            send_sync_message_to(i, 1, &dummy);
        MPI_Barrier(MPI_COMM_WORLD);
    }
    else if (mpi_rank >= first_feeder_rank && mpi_rank < first_eater_rank)
    {
        MPI_Barrier(MPI_COMM_WORLD);
        feed_targets_with(fopen("sample-input", "r"));
        MPI_Barrier(MPI_COMM_WORLD);
    }
    else if (mpi_rank >= first_eater_rank)
    {
        MPI_Barrier(MPI_COMM_WORLD);
        uint8_t recv_buffer[TARGET_BUFFER_SIZE] = {0};
        for (;;) {
            MPI_Status stat;
            MPI_Recv(recv_buffer, TARGET_BUFFER_SIZE, MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &stat);
            if (stat.MPI_SOURCE == global_coordinator)
                break;
            /* parse and add data */
            int actually_received = 0;
            MPI_Get_count(&stat, MPI_BYTE, &actually_received);
            printf("%d - received %d bytes from %d\n", mpi_rank, actually_received, stat.MPI_SOURCE);
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }

    /*
     * We have now sent info on all out chunks, and received info on all chunks
     * that we are responsible for.
     * */

    MPI_Finalize();
    return 0;
}
