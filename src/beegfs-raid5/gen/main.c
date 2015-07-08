#include <assert.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <err.h>

#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include <mpi.h>

#include "../common/common.h"
#include "../common/persistent_db.h"
#include "../common/progress_reporting.h"
#include "../common/task_processing.h"
#include "file_info_hash.h"

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
void send_sync_message_to(int recieving_rank, int msg_size, const uint8_t msg[static msg_size])
{
    MPI_Ssend((void*)msg, msg_size, MPI_BYTE, recieving_rank, 0, MPI_COMM_WORLD);
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
    int counter = 0;
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
    uint8_t failed = 0;
    send_sync_message_to(global_coordinator, sizeof(failed), &failed);
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

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_world_size);

    int ntargets = (mpi_world_size - 1)/2;

    if (ntargets > MAX_TARGETS) {
        return 1;
    }

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
    Target targetIDs[2*MAX_TARGETS] = {{0,0}};
    Target targetID = {0,0};
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
        for (int i = 1; i < 2*ntargets+1; i+=2)
            assert(targetIDs[i].id == targetIDs[i+1].id);
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
    if (mpi_rank == global_coordinator)
    {
        int still_in_stage_1 = ntargets;
        int failed = 0;
        while (still_in_stage_1 > 0) {
            MPI_Status stat;
            int8_t error;
            MPI_Recv(&error, sizeof(error), MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &stat);
            still_in_stage_1 -= 1;
            failed += error;
            if (error != 0)
                printf("gco: got message from %d, with error = %d\n", stat.MPI_SOURCE, error);
        }
        /* Inform all eaters that there is no more food */
        uint8_t dummy = 1;
        for (int i = 1; i < 1 + 2*ntargets; i+=2)
            send_sync_message_to(i, 1, &dummy);
    }
    else if (p1_feeder)
    {
        FILE *slave;
        char cmd_buf[512];
        if (strcmp(operation, "complete") == 0)
            snprintf(cmd_buf, sizeof(cmd_buf), "bp-find-all-chunks %s/chunks", store_dir);
        else if (strcmp(operation, "partial") == 0)
            snprintf(cmd_buf, sizeof(cmd_buf), "audit-find-between --from %s --to %s --store %s/chunks", timestamp_a, timestamp_b, store_dir);
        else
            strcpy(cmd_buf, "cat /dev/null");
        slave = popen(cmd_buf, "r");
        feed_targets_with(slave, ntargets);
        pclose(slave);
    }
    else if (p1_eater)
    {
        file_info_hash = fih_init();
        uint8_t recv_buffer[TARGET_BUFFER_SIZE] = {0};
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
    assert(items_received < MAX_WORKITEMS);
    qsort(received_entries, items_received, sizeof(SizeIndex), cmp_entries);
    MPI_Barrier(comm);
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
    hs.sample = &pr_sample;
    hs.fd_null = open("/dev/null", O_WRONLY);
    hs.fd_zero = open("/dev/zero", O_RDONLY);
    hs.write_dir = openat(store_fd, "parity", O_DIRECTORY | O_RDONLY);
    hs.read_chunk_dir = openat(store_fd, "chunks", O_DIRECTORY | O_RDONLY);
    hs.read_parity_dir = -1; /* We only write to parity, no reading */
    close(store_fd);

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
                if (GET_P(fi->locations) == NO_P)
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
            continue;
        }

        TaskInfo ti = { hs.read_chunk_dir, 0, -1 };
        size_t j = 0;
        const char *s = worklist_keys;
        while (j < nitems)
        {
            struct timespec tv1;
            clock_gettime(CLOCK_MONOTONIC, &tv1);
            size_t s_len = strlen(s);
            int report = process_task(&hs, s, worklist_info + j, ti);
            if (worklist_info[j].locations & L_MASK)
                pdb_set(pdb, s, s_len, worklist_info + j);
            else
                pdb_del(pdb, s, s_len);
            s += s_len + 1;
            j += 1;
            struct timespec tv2;
            clock_gettime(CLOCK_MONOTONIC, &tv2);
            double dt = (tv2.tv_sec - tv1.tv_sec) * 1.0
                + (tv2.tv_nsec - tv1.tv_nsec) * 1e-9;
            if (report) {
                pr_sample.dt += dt;
                pr_sample.nfiles += 1;
            }
            if (pr_sample.dt >= 1.0) {
                pr_add_tmp_to_total(&pr_sample);
                pr_report_progress(&pr_sender, pr_sample);
                pr_clear_tmp(&pr_sample);
            }
        }
        pr_add_tmp_to_total(&pr_sample);
        pr_report_progress(&pr_sender, pr_sample);
        pr_clear_tmp(&pr_sample);
        pr_report_done(&pr_sender);
    }

    if (hs.error != 0)
    {
        fprintf(stderr, "%s gave error %d (%s) on st %d\n",
                hs.error_path,
                hs.error,
                strerror(hs.error),
                rank2st[mpi_rank]);
    }

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
