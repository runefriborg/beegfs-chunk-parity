#include <assert.h>
#include <stdint.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#include <mpi.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common.h"
#include "task_processing.h"

#define FILE_TRANSFER_BUFFER_SIZE (10*1024*1024)

extern int st2rank[MAX_STORAGE_TARGETS];

/* Replicates a mkdir -p/--parents command for the dir the filename is in */
static void mkdir_for_file(const char *filename) {
    char tmp[256];
    strncpy(tmp, filename, sizeof(tmp));
    for(char *p = tmp + 1; *p; p++) {
        if(*p == '/') {
            *p = 0;
            mkdir(tmp, S_IRWXU);
            *p = '/';
        }
    }
}

static
void send_sync_message_to(int recieving_rank, int msg_size, const uint8_t msg[static msg_size])
{
    MPI_Send((void*)msg, msg_size, MPI_BYTE, recieving_rank, 0, MPI_COMM_WORLD);
}
static
void recv_sync_message_from(int sending_rank, int size, void *dst)
{
    MPI_Status stat;
    MPI_Recv(dst, size, MPI_BYTE, sending_rank, 0, MPI_COMM_WORLD, &stat);
}

static
void path_with_subst(char *res, size_t len, const char *path, const char *pat)
{
    size_t i = 0;
    for (; pat[i] != '\0'; i++)
        res[i] = (pat[i] == ' '? path[i] : pat[i]);
    for (; path[i] != '\0' && i < len; i++)
        res[i] = path[i];
    res[i] = '\0';
}

static
int open_fileid_readonly(const char *id, const char *load_pat)
{
    if (strncmp("/store0", id, 7) != 0) {
        fputs("ERROR: All input files must start with /store0!"
                " Reading from /dev/zero.\n", stderr);
        return open("/dev/zero", O_RDONLY);
    }
    char tmp[256];
    path_with_subst(tmp, strlen(id), id, load_pat);
    int fd = open(tmp, O_RDONLY);
    if (fd <= 0)
        printf("opened '%s' with error = '%s'\n", tmp, strerror(errno));
    if (fd < 0)
        return -errno;
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL | POSIX_FADV_WILLNEED);
    return fd;
}

static
int open_fileid_new_parity(const char *id, ssize_t expected_size, const char *save_pat)
{
    if (strncmp("/store0", id, 7) != 0) {
        fputs("ERROR: All input files must start with /store0!"
                " Writing to /dev/null.\n", stderr);
        return open("/dev/null", O_WRONLY);
    }
    char tmp[256];
    path_with_subst(tmp, strlen(id), id, save_pat);
    mkdir_for_file(tmp);
    int fd = creat(tmp, S_IRUSR | S_IWUSR);
    if (fd < 0)
        return -errno;
    posix_fallocate(fd, 0, expected_size);
    return fd;
}

#define P_rank(fi) (st2rank[GET_P((fi)->locations)])

static
int active_ranks(uint64_t locations)
{
    /* gcc specific function to count number of ones */
    return __builtin_popcountll(locations & L_MASK);
}

static
uint64_t div_round_up(uint64_t a, uint64_t b)
{
    return (a + (b - 1)) / b;
}

static
void xor_parity(uint8_t *restrict dst, size_t nbytes, const uint8_t *data, int nsources)
{
    memcpy(dst, data, nbytes);
    for (int j = 1; j < nsources; j++)
    {
        const uint8_t *src = data + j*nbytes; 
        size_t i = 0;
        for (; i + 8 < nbytes; i += 8)
            *(uint64_t *)(dst + i) ^= *(uint64_t *)(src + i);
        for (; i < nbytes; i++)
            dst[i] ^= src[i];
    }
}

/*
 * Roles:
 *  chunk_sender - open file and start sending parts to P-rank
 *  parity_generator:
 *      receives data from chunk sources, calculate and store parity
 */
static
void parity_generator(const char *path, const FileInfo *task, TaskInfo ti, int my_st, size_t *nbytes)
{
#define IRECV_ALL(ii, loc, size) do { \
    for (int ii = 0; ii < active_source_ranks; ii++) \
        MPI_Irecv((loc), (size), MPI_BYTE, ranks[ii], \
                0, MPI_COMM_WORLD, &source_messages[ii]); \
    } while(0)
#define SEND_ALL(data, data_size) do { \
    for (int ii = 0; ii < active_source_ranks; ii++) \
        MPI_Isend((data), (data_size), MPI_BYTE, ranks[ii], \
                0, MPI_COMM_WORLD, &source_messages[ii]); \
    MPI_Waitall(active_source_ranks, source_messages, source_stat); \
    } while(0)

    MPI_Request source_messages[MAX_STORAGE_TARGETS];
    MPI_Status source_stat[MAX_STORAGE_TARGETS];
    const int active_source_ranks = active_ranks(task->locations);
    int ranks[MAX_STORAGE_TARGETS];
    for (int i = 0, j = 0; i < MAX_STORAGE_TARGETS; i++)
        if (TEST_BIT(task->locations, i))
            ranks[j++] = st2rank[i];

    uint64_t chunk_sizes[MAX_STORAGE_TARGETS];
    /* When rebuilding we need the stored chunk sizes from the parity block on
     * ti.actual_P_st */
    if (ti.is_rebuilding)
    {
        recv_sync_message_from(
                st2rank[ti.actual_P_st],
                active_source_ranks*sizeof(uint64_t),
                chunk_sizes);
    }
    else
    {
        IRECV_ALL(src, chunk_sizes + src, sizeof(uint64_t));
        MPI_Waitall(active_source_ranks, source_messages, source_stat);
    }

    uint64_t max_cs = 0;
    for (int i = 0; i < active_source_ranks; i++)
        max_cs = MAX(max_cs, chunk_sizes[i]);
    SEND_ALL(&max_cs, sizeof(max_cs));
    *nbytes = *nbytes + max_cs;

    uint8_t *data_a = malloc(active_source_ranks * FILE_TRANSFER_BUFFER_SIZE);
    uint8_t *data_b = malloc(active_source_ranks * FILE_TRANSFER_BUFFER_SIZE);
    uint8_t *P_block = malloc(FILE_TRANSFER_BUFFER_SIZE);
    uint64_t data_left = max_cs;
    size_t buffer_size = MIN(FILE_TRANSFER_BUFFER_SIZE, max_cs);
    int expected_messages = div_round_up(max_cs, FILE_TRANSFER_BUFFER_SIZE);
    int P_fd = open_fileid_new_parity(path, max_cs + active_source_ranks*8, ti.save_pat);
    int P_local_write_error = (P_fd < 0);

    /* If we are not rebuilding, we store all chunk sizes at the start of the
     * parity file. */
    if (!ti.is_rebuilding)
        P_local_write_error |= (write(P_fd, chunk_sizes, sizeof(uint64_t)*active_source_ranks) <= 0);

    for (int msg_i = 0; msg_i < expected_messages; msg_i++)
    {
        if (msg_i == 0)
            IRECV_ALL(src, data_a + src*buffer_size, buffer_size);
        MPI_Waitall(active_source_ranks, source_messages, source_stat);
        if (msg_i + 1 != expected_messages)
            IRECV_ALL(src, data_b + src*buffer_size, buffer_size);
        /* calculate P and write to disk while waiting for next data chunk */
        xor_parity(P_block, buffer_size, data_a, active_source_ranks);
        if (!P_local_write_error) {
            ssize_t wsize = MIN(buffer_size, data_left);
            ssize_t w = write(P_fd, P_block, wsize);
            data_left -= wsize;
            P_local_write_error |= (w <= 0);
        }
        uint8_t *tmp = data_a;
        data_a = data_b;
        data_b = tmp;
    }

    if (ti.is_rebuilding) {
        uint64_t loc = task->locations & ~(1 << ti.actual_P_st) & L_MASK;
        uint64_t my_mask = (1 << my_st) - 1; /* 1's up to my_st */
        int my_index = active_ranks(loc & my_mask);
        ftruncate(P_fd, chunk_sizes[my_index]);
    }

    free(P_block);
    free(data_a);
    free(data_b);
    close(P_fd);
#undef SEND_ALL
#undef IRECV_ALL
}

static
void chunk_sender(const char *path, const FileInfo *task, TaskInfo ti, int my_st, size_t *nbytes)
{
    int coordinator = P_rank(task);
    int ntargets = active_ranks(task->locations);
    int have_had_error = 0;
    int fd = open_fileid_readonly(path, ti.load_pat);
    uint64_t fd_size = 0;
    if (fd < 0)
        have_had_error = 1;
    else {
        struct stat st;
        fstat(fd, &st);
        fd_size = st.st_size;
        if (ti.is_rebuilding && ti.actual_P_st == my_st)
            fd_size -= ntargets*sizeof(uint64_t);
    }
    *nbytes = *nbytes + fd_size;

    if (ti.is_rebuilding && ti.actual_P_st == my_st) {
        uint64_t chunk_sizes[MAX_STORAGE_TARGETS];
        read(fd, chunk_sizes, ntargets*sizeof(uint64_t));
        send_sync_message_to(coordinator, ntargets*sizeof(uint64_t), (uint8_t*)chunk_sizes);
    }
    else if (!ti.is_rebuilding)
        send_sync_message_to(coordinator, sizeof(fd_size), (uint8_t *)&fd_size);

    uint64_t data_in_fd = 0;
    recv_sync_message_from(coordinator, sizeof(data_in_fd), &data_in_fd);

    size_t buffer_size = MIN(FILE_TRANSFER_BUFFER_SIZE, data_in_fd);
    uint8_t *data = malloc(FILE_TRANSFER_BUFFER_SIZE);

    size_t read_from_fd = 0;
    while (read_from_fd < data_in_fd)
    {
        size_t data_left = data_in_fd - read_from_fd;
        if (!have_had_error) {
            ssize_t r = read(fd, data, MIN(buffer_size, data_left));
            have_had_error |= (r <= 0);
            if (have_had_error)
                memset(data, 0, buffer_size);
            if (r > 0 && (size_t)r < buffer_size)
                memset(data + r, 0, (buffer_size - r));
        }
        read_from_fd += buffer_size;
        send_sync_message_to(coordinator, buffer_size, data);
    }

    free(data);
    close(fd);
}

/* Returns non-zero if we are involved in the task */
int process_task(int my_st, const char *path, const FileInfo *fi, TaskInfo ti, size_t *nbytes)
{
    if (GET_P(fi->locations) == NO_P)
        return 0;

    if (GET_P(fi->locations) == my_st)
        parity_generator(path, fi, ti, my_st, nbytes);
    else if (my_st >= 0 && TEST_BIT(fi->locations, my_st))
        chunk_sender(path, fi, ti, my_st, nbytes);
    else
        return 0;
    return 1;
}
