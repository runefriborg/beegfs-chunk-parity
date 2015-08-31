#include <assert.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <err.h>
#include <errno.h>

#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include <pthread.h>

#include <mpi.h>

#include "../common/common.h"
#include "../common/persistent_db.h"
#include "../common/progress_reporting.h"
#include "../common/task_processing.h"
#include "file_info_hash.h"
#include "assign_lanes.h"

#define MAX_TARGETS MAX_STORAGE_TARGETS
#define TARGET_BUFFER_SIZE (10*1024*1024)
#define TARGET_SEND_THRESHOLD (1*1024*1024)

#define PROF_START(name) \
    struct timespec t_##name##_0; \
    clock_gettime(CLOCK_MONOTONIC, &t_##name##_0)
#define PROF_END(name) \
    struct timespec t_##name##_1; \
    clock_gettime(CLOCK_MONOTONIC, &t_##name##_1)
#define PROF_VAL(name) \
    ((t_##name##_1.tv_sec - t_##name##_0.tv_sec) * 1.0 \
    + (t_##name##_1.tv_nsec - t_##name##_0.tv_nsec) * 1e-9)

static const int global_coordinator = 0;
static int mpi_rank;
static int mpi_world_size;

int st2rank[MAX_STORAGE_TARGETS];
int rank2st[MAX_STORAGE_TARGETS*2+1];

static
void send_sync_message_to(int recieving_rank, int msg_size, void *msg)
{
    MPI_Send(msg, msg_size, MPI_BYTE, recieving_rank, 0, MPI_COMM_WORLD);
}

static
int sts_in_use(uint64_t locations)
{
    /* gcc specific function to count number of ones */
    return __builtin_popcountll(locations & L_MASK);
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
    return st2rank[storage_target];
}
static
int st_from_feeder_rank(int feeder)
{
    assert((feeder > 0) && (feeder % 2 == 0));
    return rank2st[feeder];
}

/*
 * Combine the two `locations` fields.
 * The P value is only copied over if it isn't present in `dst->locations`
 */
static void fill_in_missing_fields(FileInfo *dst, const FileInfo *src)
{
    uint64_t old_P = GET_P(src->locations);
    dst->locations = (dst->locations | src->locations) & L_MASK;
    if (TEST_BIT(dst->locations, old_P) == 0)
        dst->locations = WITH_P(dst->locations, old_P);
    else
        dst->locations = WITH_P(dst->locations, NO_P);
}

/*
 * Selecting the P rank is done by hashing the path once and then iteratively
 * hashing the hash result until we can map it to a rank that is not already
 * mentioned in the list of locations.
 * Could be done smarter -- for one there is no guarantee this terminates.
 */
static
void select_P(const char *path, FileInfo *fi, unsigned ntargets)
{
    if (sts_in_use(fi->locations) == (int)ntargets)
        return;
    unsigned H = simple_hash(path, strlen(path));
choose_P_again:
    H = H ^ simple_hash((const char *)&H, sizeof(H));
    uint64_t P = H % ntargets;
    if (TEST_BIT(fi->locations, P))
        goto choose_P_again;
    fi->locations = WITH_P(fi->locations, P);
}

typedef struct {
    HostState *hs;
    PersistentDB *pdb;
    const char *worklist_keys;
    FileInfo *worklist_info;
    int *worklist_lanes;
    size_t nitems;
    ProgressSample *sample;
    int *working_counter;
    pthread_mutex_t *lock;
    int lane;
    int nlanes;
} ListParams;

static
void *process_list(void *p)
{
    ListParams *params = (ListParams *)p;
    HostState *hs = params->hs;
    assert(hs);
    FileInfo *worklist_info = params->worklist_info;
    assert(worklist_info);
    PersistentDB *pdb = params->pdb;
    assert(pdb);
    TaskInfo ti = { hs->read_chunk_dir, 0, -1, params->lane, params->sample };
    const char *s = params->worklist_keys;
    assert(s != NULL);
    int lane = params->lane;
    size_t nitems = params->nitems;
    for (size_t i = 0; i < nitems; i++)
    {
        struct timespec tv1;
        clock_gettime(CLOCK_MONOTONIC, &tv1);

        const char *val = s;
        size_t len = strlen(val);
        s += len + 1;

        if (params->worklist_lanes[i] != lane)
            continue;

        int report = process_task(hs, val, worklist_info + i, ti);
        if (worklist_info[i].locations & L_MASK)
            pdb_set(pdb, val, len, worklist_info + i);
        else
            pdb_del(pdb, val, len);

        struct timespec tv2;
        clock_gettime(CLOCK_MONOTONIC, &tv2);
        double new_dt = (tv2.tv_sec - tv1.tv_sec) * 1.0
            + (tv2.tv_nsec - tv1.tv_nsec) * 1e-9;
        if (report) {
            params->sample->dt += new_dt;
            params->sample->nfiles += 1;
        }
    }
    pthread_mutex_lock(params->lock);
    *params->working_counter = *params->working_counter - 1;
    pthread_mutex_unlock(params->lock);
    return NULL;
}

typedef struct {
    int64_t timestamp;
    uint64_t chunk_size;
    uint64_t path_len;
    uint64_t event_type;
    char path[];
} packed_file_info;

typedef struct {
    uint64_t size;
    uint64_t idx;
} SizeIndex;

static
int cmp_entries(const void *pa, const void *pb)
{
    uint64_t a = ((SizeIndex *)pa)->size;
    uint64_t b = ((SizeIndex *)pb)->size;
    if (a < b)
        return -1;
    if (a == b)
        return 0;
    return 1;
}

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
void push_to_target(int target, const char *path, int path_len, int64_t timestamp, uint64_t chunk_size, uint8_t event_type)
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

    packed_file_info finfo = {timestamp, chunk_size, path_len, event_type};
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
void feed_targets_with(FILE *input_file, unsigned ntargets)
{
    char buf[64*1024];
    ssize_t buf_size = sizeof(buf);
    ssize_t buf_offset = 0;
    int read;
    int64_t counter = 0;
    while ((read = fread(buf + buf_offset, 1, buf_size - buf_offset, input_file)) > 0)
    {
        size_t buf_alive = buf_offset + read;
        const char *bufp = buf;
        while (buf_alive >= 4*sizeof(uint64_t)) {
            int64_t timestamp_secs = ((int64_t *)bufp)[0];
            uint64_t chunk_size = ((uint64_t *)bufp)[1];
            uint64_t event_type = ((uint64_t *)bufp)[2];
            uint64_t len_of_path = ((uint64_t *)bufp)[3];
            if (4*sizeof(uint64_t) + len_of_path > buf_alive) {
                buf_offset = buf_alive;
                memmove(buf, bufp, buf_alive);
                break;
            }
            const char *path = bufp + 4*sizeof(uint64_t);
            unsigned st = (simple_hash(path, len_of_path)) % ntargets;
            push_to_target(
                    st,
                    path,
                    len_of_path,
                    timestamp_secs,
                    chunk_size,
                    event_type);
            counter += 1;
            if (counter >= 10000) {
                send_sync_message_to(global_coordinator, sizeof(counter), &counter);
                counter = 0;
            }
            bufp += len_of_path + 4*sizeof(uint64_t);
            buf_alive -= len_of_path + 4*sizeof(uint64_t);
        }
        if (4*sizeof(uint64_t) >= buf_alive) {
            buf_offset = buf_alive;
            memmove(buf, bufp, buf_alive);
        }
    }
    send_remaining_data_to_targets();
    /* tell global-coordinator that we are done */
    send_sync_message_to(global_coordinator, sizeof(counter), &counter);
    int64_t msg = -1;
    send_sync_message_to(global_coordinator, sizeof(msg), &msg);
}

/* Really minimal PCG32 code / (c) 2014 M.E. O'Neill / pcg-random.org *
 * Licensed under Apache License 2.0 (NO WARRANTY, etc. see website)  */
typedef struct { uint64_t state;  uint64_t inc; } pcg32_random_t;

static uint32_t pcg32_random_r(pcg32_random_t* rng)
{
    uint64_t oldstate = rng->state;
    rng->state = oldstate * 6364136223846793005ULL + (rng->inc|1);
    uint32_t xorshifted = ((oldstate >> 18u) ^ oldstate) >> 27u;
    uint32_t rot = oldstate >> 59u;
    return (xorshifted >> rot) | (xorshifted << ((-rot) & 31));
}
/* -- end of PCG32 code -- */

static
void shuffle(SizeIndex *array, size_t n)
{
    if (n <= 1)
        return;
    pcg32_random_t rng = {0x853c49e6748fea9bULL, 0xda3e39cb94b95bdbULL};
    for (size_t i = n - 1; i > 0; i--) /* i = (n-1),(n-2),...,1 */
    {
        size_t j = pcg32_random_r(&rng) % (i + 1);
        SizeIndex t = array[j];
        array[j] = array[i];
        array[i] = t;
    }
}

int main(int argc, char **argv)
{
    if (argc != 7) {
        fputs("We need 6 arguments\n", stdout);
        return 1;
    }

    const char *operation = argv[1];
    const char *store_dir = argv[2];
    const char *timestamp_a = argv[3];
    const char *timestamp_b = argv[4];
    const char *data_file = argv[5];
    const char *db_folder = argv[6];

    PROF_START(total);

    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fputs("Your MPI does not support multithreading!\n", stderr);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_world_size);

    int ntargets = (mpi_world_size - 1)/2;

    if (ntargets > MAX_TARGETS) {
        return 1;
    }

    /* TODO: Should be skipped on rank 0 - it doesn't have a /store0x */
    int store_fd = open(store_dir, O_DIRECTORY | O_RDONLY);

    PROF_START(init);

    int last_run_fd = -1;
    RunData last_run;
    memset(&last_run, 0, sizeof(RunData));
    if (mpi_rank == 0) {
        last_run_fd = open(data_file, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        read(last_run_fd, &last_run, sizeof(RunData));
    }

    /* Create mapping from storage targets to ranks, and vice versa */
    Target targetIDs[2*MAX_TARGETS] = {{0,0,0}};
    Target targetID = {0,0,GIT_VERSION};
    if (mpi_rank != 0)
    {
        int target_ID_fd = openat(store_fd, "targetNumID", O_RDONLY);
        char targetID_s[20] = {0};
        read(target_ID_fd, targetID_s, sizeof(targetID_s));
        close(target_ID_fd);
        targetID.id = atoi(targetID_s);
        targetID.rank = mpi_rank;
    }
    MPI_Gather(
            &targetID, sizeof(Target), MPI_BYTE,
            targetIDs, sizeof(Target), MPI_BYTE,
            0,
            MPI_COMM_WORLD);
    if (mpi_rank == 0) {
        for (int i = 1; i < 2*ntargets+1; i++)
            if (targetIDs[i].version != GIT_VERSION)
                errx(1, "Version mismatch");
        for (int i = 0; i < ntargets; i++)
            targetIDs[i] = targetIDs[2*i+1];
        int k = last_run.ntargets;
        for (int i = 0; i < last_run.ntargets; i++)
            last_run.targetIDs[i].rank = -1;
        for (int i = 0; i < ntargets; i++) {
            Target target = targetIDs[i];
            int j = 0;
            for (; j < last_run.ntargets; j++)
                if (last_run.targetIDs[j].id == target.id) {
                    last_run.targetIDs[j] = target;
                    break;
                }
            if (j + 1 >= last_run.ntargets)
                last_run.targetIDs[k++] = target;
        }
        last_run.ntargets = ntargets;
        rank2st[0] = -1;
        for (int i = 0; i < ntargets; i++)
        {
            st2rank[i] = last_run.targetIDs[i].rank;
            rank2st[st2rank[i]] = i;
            rank2st[st2rank[i]+1] = i;
        }
    }
    MPI_Bcast(st2rank, sizeof(st2rank), MPI_BYTE, 0, MPI_COMM_WORLD);
    MPI_Bcast(rank2st, sizeof(rank2st), MPI_BYTE, 0, MPI_COMM_WORLD);

    if (mpi_rank == 0) {
        write(last_run_fd, &last_run, sizeof(RunData));
        close(last_run_fd);
    }

    PROF_END(init);

    int eater_ranks[MAX_TARGETS];
    int feeder_ranks[MAX_TARGETS];
    for (int i = 0; i < ntargets; i++) {
        eater_ranks[i] = 1 + 2*i;
        feeder_ranks[i] = 2 + 2*i;
    }

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
    MPI_Group_excl(everyone, ntargets, feeder_ranks, &not_everyone);
    MPI_Comm comm;
    MPI_Comm_create(MPI_COMM_WORLD, not_everyone, &comm);

    PROF_START(phase1);

#ifndef MAX_WORKITEMS
#define MAX_WORKITEMS (1ULL*1000*1000)
#endif
    FileInfoHash *file_info_hash = NULL;
    size_t name_bytes_written = 0;
    const size_t name_bytes_limit = MAX_WORKITEMS*100;
    char *flat_file_names = malloc(name_bytes_limit);
    char **names = malloc(MAX_WORKITEMS*sizeof(char*));
    FatFileInfo *file_info = malloc(MAX_WORKITEMS*sizeof(FatFileInfo));
    SizeIndex *received_entries = malloc(MAX_WORKITEMS*sizeof(SizeIndex));
    size_t items_received = 0;

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
    int p1_eater = (mpi_rank % 2 == 1);
    int p1_feeder = (mpi_rank > 0 && mpi_rank % 2 == 0);
    int64_t files_seen_total = 0;
    int outputs_on_line = 0;
    if (mpi_rank == global_coordinator)
    {
        int still_in_stage_1 = ntargets;
        printf("chunks: ");
        while (still_in_stage_1 > 0) {
            MPI_Status stat;
            int64_t files_seen;
            MPI_Recv(&files_seen, sizeof(files_seen), MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &stat);
            if (files_seen < 0)
                still_in_stage_1 -= 1;
            else {
                files_seen_total += files_seen;
                printf("  %ld", files_seen_total);
                if (++outputs_on_line == 12)
                {
                    printf("\n");
                    outputs_on_line = 0;
                }
            }
        }
        /* Inform all eaters that there is no more food */
        uint8_t dummy = 1;
        for (int i = 1; i < 1 + 2*ntargets; i+=2)
            send_sync_message_to(i, 1, &dummy);
        printf("\nTotal number of chunks found: %8ld\n", files_seen_total);
    }
    else if (p1_feeder)
    {
        FILE *slave;
        char cmd_buf[512];
        if (strcmp(operation, "complete") == 0)
            snprintf(cmd_buf, sizeof(cmd_buf), "bp-find-all-chunks %s/chunks", store_dir);
        else if (strcmp(operation, "partial") == 0)
            snprintf(cmd_buf, sizeof(cmd_buf), "bp-find-chunks-changed-between --from %s --to %s --store %s/chunks", timestamp_a, timestamp_b, store_dir);
        else
            strcpy(cmd_buf, "cat /dev/null");
        slave = popen(cmd_buf, "r");
        feed_targets_with(slave, ntargets);
        pclose(slave);
    }
    else if (p1_eater)
    {
        file_info_hash = fih_init();
        uint8_t *recv_buffer = calloc(1,TARGET_BUFFER_SIZE);
        for (;;) {
            MPI_Status stat;
            MPI_Recv(recv_buffer, TARGET_BUFFER_SIZE, MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &stat);
            if (stat.MPI_SOURCE == global_coordinator)
                break;
            /* parse and add data */
            int actually_received = 0;
            MPI_Get_count(&stat, MPI_BYTE, &actually_received);
            int src = stat.MPI_SOURCE;
            int i = 0;
            while (i < actually_received) {
                packed_file_info *pfi = (packed_file_info *)(recv_buffer+i);
                i += sizeof(packed_file_info) + pfi->path_len;
                if (name_bytes_written + pfi->path_len + 1 >= name_bytes_limit)
                    errx(1, "Only room for %zu bytes of paths. Asked for %lu.",
                            name_bytes_limit, name_bytes_written + pfi->path_len + 1);
                char *n = flat_file_names + name_bytes_written;
                memmove(n, pfi->path, pfi->path_len);
                n[pfi->path_len] = '\0';
                name_bytes_written += pfi->path_len + 1;
                size_t idx;
                if (fih_get_or_create(file_info_hash, n, &idx) == FIH_NEW) {
                    memset(&file_info[idx], 0, sizeof(FatFileInfo));
                    received_entries[idx].size = 0;
                    received_entries[idx].idx = idx;
                    names[idx] = n;
                    items_received += 1;
                    if (items_received >= MAX_WORKITEMS)
                        errx(1, "Too many chunks (max = %llu)", MAX_WORKITEMS);
                }
                else
                    name_bytes_written -= pfi->path_len + 1;
                received_entries[idx].size += pfi->chunk_size;
                fih_add_info(
                        &file_info[idx],
                        st_from_feeder_rank(src),
                        pfi->timestamp,
                        (pfi->event_type == UNLINK_EVENT));
            }
        }
        free(recv_buffer);
        fih_term(file_info_hash);
        file_info_hash = NULL;
    }

    if (p1_feeder) {
        MPI_Finalize();
        return 0;
    }

    PROF_END(phase1);

    PROF_START(sort_by_size);
    MPI_Barrier(comm);
    if (mpi_rank == 0) {
        printf("Starting sort..");
        fflush(stdout);
    }
    assert(items_received < MAX_WORKITEMS);
    shuffle(received_entries, items_received);
    qsort(received_entries, items_received, sizeof(SizeIndex), cmp_entries);
    MPI_Barrier(comm);
    if (mpi_rank == 0)
        printf("  done.\n");
    PROF_END(sort_by_size);

    PROF_START(load_db);
    PersistentDB *pdb = pdb_init(db_folder);
    PROF_END(load_db);

    PROF_START(phase2);

    FileInfo *worklist_info = malloc(MAX_WORKITEMS*sizeof(FileInfo));
    char *worklist_keys = malloc(name_bytes_limit);

    int mpi_bcast_rank;
    MPI_Comm_rank(comm, &mpi_bcast_rank);
    int mpi_bcast_size;
    MPI_Comm_size(comm, &mpi_bcast_size);
    int my_st = rank2st[mpi_rank];

    ProgressSender pr_sender;
    memset(&pr_sender, 0, sizeof(pr_sender));
    ProgressSample pr_sample = PROGRESS_SAMPLE_INIT;
    HostState hs;
    memset(&hs, 0, sizeof(hs));
    hs.storage_target = my_st;
    hs.fd_null = open("/dev/null", O_WRONLY);
    hs.fd_zero = open("/dev/zero", O_RDONLY);
    char *log_file_name = calloc(1, 201);
    snprintf(log_file_name, 200, "%s/../errors.log", db_folder);
    hs.log = fopen(log_file_name, "w");
    hs.write_dir = openat(store_fd, "parity", O_DIRECTORY | O_RDONLY);
    hs.read_chunk_dir = openat(store_fd, "chunks", O_DIRECTORY | O_RDONLY);
    hs.read_parity_dir = -1; /* We only write to parity, no reading */
    close(store_fd);

    fprintf(hs.log, "=== start new run ===\n");

    for (int i = 1; i < mpi_bcast_size; i++)
    {
        size_t nitems = items_received;
        size_t path_bytes = 0;
        if (mpi_bcast_rank == i)
        {
            /*
             * Collect all file info entries in to a packed array that is ready for
             * broadcasting.
             * */
            for (size_t j = 0; j < nitems; j++)
            {
                const char *s = names[received_entries[j].idx];
                size_t s_len = strlen(s);
                FileInfo prev_fi;
                FatFileInfo new_fi = file_info[received_entries[j].idx];
                FileInfo *fi = worklist_info + j;
                fi->timestamp = new_fi.timestamp;
                fi->locations = WITH_P(new_fi.modified, NO_P);
                int has_an_old_version = pdb_get(pdb, s, s_len, &prev_fi);
                if (has_an_old_version)
                    fill_in_missing_fields(fi, &prev_fi);
                fi->locations &= ~new_fi.deleted;
                if (P_IS_INVALID(fi->locations))
                    select_P(s, fi, (unsigned)ntargets);
                memcpy(worklist_keys + path_bytes, s, s_len + 1);
                path_bytes += s_len + 1;
            }
            assert(path_bytes == name_bytes_written);
        }
        MPI_Bcast(&nitems,       sizeof(nitems),          MPI_BYTE, i, comm);
        MPI_Bcast(worklist_info, sizeof(FileInfo)*nitems, MPI_BYTE, i, comm);
        MPI_Bcast(&path_bytes,   sizeof(path_bytes),      MPI_BYTE, i, comm);
        MPI_Bcast(worklist_keys, path_bytes,              MPI_BYTE, i, comm);

        if (nitems == 0)
            continue;

        if (mpi_rank == 0) {
            printf("\n==== begin iteration with %zu files ====\n", nitems);
            printf("st - total files   | data read     | data written  | disk I/O\n");
            pr_receive_loop(mpi_bcast_size-1);
            MPI_Barrier(comm);
            continue;
        }

#define N_LANES 12
        int *lanes = malloc(nitems*sizeof(int));
        assign_lanes(N_LANES, nitems, worklist_info, lanes);
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
        pthread_t threads[N_LANES];
        ProgressSample old_samples[N_LANES];
        ProgressSample cur_samples[N_LANES];
        memset(old_samples, 0, sizeof(old_samples));
        memset(cur_samples, 0, sizeof(cur_samples));
        int *threads_working = calloc(1,sizeof(int));
        *threads_working = N_LANES;
        pthread_mutex_t finish_lock = PTHREAD_MUTEX_INITIALIZER;
        ListParams param0 = {&hs,pdb,worklist_keys,worklist_info,lanes,nitems,NULL,threads_working,&finish_lock,0,N_LANES};
        ListParams params[N_LANES];
        for (int j = 0; j < N_LANES; j++) {
            params[j] = param0;
            params[j].sample = &cur_samples[j];
            params[j].lane = j;
            int rc = pthread_create(&threads[j], &attr, process_list, &params[j]);
            if (rc)
                errx(1, "Thread create failed (rc = %d)", rc);
        }
        pthread_attr_destroy(&attr);
        for (;;)
        {
            for (int r = 0; r < 100; r++)
            {
                pthread_mutex_lock(&finish_lock);
                if (*threads_working == 0)
                    goto after_reporting_loop;
                pthread_mutex_unlock(&finish_lock);
                usleep(10*1000);
            }
            for (int j = 0; j < N_LANES; j++) {
                size_t nf = cur_samples[j].nfiles;
                double ndt = cur_samples[j].dt;
                size_t nbw = cur_samples[j].bytes_written;
                size_t nbr = cur_samples[j].bytes_read;
                pr_sample.nfiles += nf - old_samples[j].nfiles;
                pr_sample.dt = 1.0;//ndt - old_samples[j].dt;
                pr_sample.bytes_written += nbw - old_samples[j].bytes_written;
                pr_sample.bytes_read += nbr - old_samples[j].bytes_read;
                old_samples[j].nfiles = nf;
                old_samples[j].dt = ndt;
                old_samples[j].bytes_written = nbw;
                old_samples[j].bytes_read = nbr;
            }
            pr_add_tmp_to_total(&pr_sample);
            pr_report_progress(&pr_sender, pr_sample);
            pr_clear_tmp(&pr_sample);
        }
after_reporting_loop:
        pthread_mutex_unlock(&finish_lock);
        for (int j = 0; j < N_LANES; j++) {
            void *status;
            int rc = pthread_join(threads[j], &status);
            if (rc)
                errx(1, "Thread join error (rc = %d) on thread %d", rc, j);
            pr_sample.nfiles += cur_samples[j].nfiles - old_samples[j].nfiles;
            pr_sample.dt += cur_samples[j].dt - old_samples[j].dt;
            pr_sample.bytes_written += cur_samples[j].bytes_written - old_samples[j].bytes_written;
            pr_sample.bytes_read += cur_samples[j].bytes_read - old_samples[j].bytes_read;
        }
        pr_add_tmp_to_total(&pr_sample);
        pr_report_progress(&pr_sender, pr_sample);
        pr_clear_tmp(&pr_sample);
        pr_report_done(&pr_sender);
        MPI_Barrier(comm);
    }

    if (hs.error != 0)
    {
        fprintf(hs.log, "started using zero/null after '%s' gave error %d (%s) on st %d\n",
                hs.error_path,
                hs.error,
                strerror(hs.error),
                rank2st[mpi_rank]);
    }

    fclose(hs.log);
    pdb_term(pdb);
    pdb = NULL;
    free(flat_file_names);
    free(worklist_info);
    free(worklist_keys);

    PROF_END(phase2);
    PROF_END(total);

    if (mpi_rank == 0) {
        printf("Overall timings: \n");
        printf("init         | %9.2f ms\n", 1e3*PROF_VAL(init));
        printf("phase1       | %9.2f ms\n", 1e3*PROF_VAL(phase1));
        printf("sort_by_size | %9.2f ms\n", 1e3*PROF_VAL(sort_by_size));
        printf("load_db      | %9.2f ms\n", 1e3*PROF_VAL(load_db));
        printf("phase2       | %9.2f ms\n", 1e3*PROF_VAL(phase2));
        printf("total        | %9.2f ms\n", 1e3*PROF_VAL(total));
    }

    MPI_Finalize();
    return 0;
}
