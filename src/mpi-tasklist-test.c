#include <assert.h>
#include <stdint.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include <mpi.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define FILE_TRANSFER_BUFFER_SIZE (1*1024*1024)

static int mpi_rank;
static int mpi_world_size;
static char hostname[512];

void send_sync_message_to(int16_t recieving_rank, int msg_size, const uint8_t msg[static msg_size])
{
    MPI_Send((void*)msg, msg_size, MPI_BYTE, recieving_rank, 0, MPI_COMM_WORLD);
}

void receive_sync_message_from(int16_t sending_rank, int msg_size, uint8_t msg[static msg_size])
{
    MPI_Status stat;
    MPI_Recv(msg, msg_size, MPI_BYTE, sending_rank, 0, MPI_COMM_WORLD, &stat);
}

#define MAX_CHUNKS_PER_FILE 8
typedef int64_t FileID;
typedef struct {
    FileID file_id;
    uint64_t full_file_size;
    int16_t sources[MAX_CHUNKS_PER_FILE];
    int16_t P_rank;
    int16_t Q_rank;
} Task;

int open_fileid_readonly(FileID id)
{
    (void) id;
    int fd = open("/dev/zero", O_RDONLY);
    if (fd < 0)
        return -errno;
    return fd;
}
int open_fileid_new_parity(FileID id)
{
    (void) id;
    int fd = open("/dev/null", O_WRONLY);
    if (fd < 0)
        return -errno;
    return fd;
}


/*
 * Roles:
 *  parity_receiver - simply receives parts of Q from the P-rank
 *  chunk_sender - open file_id and starts sending parts to P-rank
 *  parity_generator:
 *      receives chunks sources, does parity and sends parts of Q to Q-rank
 */
void parity_generator(Task task)
{
    int16_t Q_rank = task.Q_rank;
    const int active_source_ranks = 2; /* calculated from sources != -1 and total file size? */
    uint8_t *data = calloc(active_source_ranks, FILE_TRANSFER_BUFFER_SIZE);
    uint8_t *Q_data = calloc(1, FILE_TRANSFER_BUFFER_SIZE);
    int P_fd = open_fileid_new_parity(task.file_id);
    int P_local_write_error = (P_fd < 0);
    int expected_messages = task.full_file_size / active_source_ranks / FILE_TRANSFER_BUFFER_SIZE;
    MPI_Status stat;
    MPI_Request last_msg_to_Q;
    MPI_Request source_messages[active_source_ranks];
    MPI_Status source_stat[active_source_ranks];
    for (int msg_i = 0; msg_i < expected_messages; msg_i++)
    {
        /* begin async receive from all sources and wait for all receives to
         * finish. Ideally we would do calculations and local I/O on previously
         * received data before waiting -- but this is only a first draft. */
        for (int i = 0; i < active_source_ranks; i++) {
            MPI_Irecv(
                    data + i*FILE_TRANSFER_BUFFER_SIZE,
                    FILE_TRANSFER_BUFFER_SIZE,
                    MPI_BYTE,
                    task.sources[i],
                    0,
                    MPI_COMM_WORLD,
                    &source_messages[i]);
        }
        MPI_Waitall(active_source_ranks, source_messages, source_stat);
        /* calculate P/Q and write P to disk */
        if (!P_local_write_error) {
            ssize_t w = write(P_fd, data, FILE_TRANSFER_BUFFER_SIZE);
            P_local_write_error |= (w <= 0);
        }
        /* send Q to Q_rank */
        if (msg_i != 0)
            MPI_Wait(&last_msg_to_Q, &stat);
        MPI_Isend(
                Q_data,
                FILE_TRANSFER_BUFFER_SIZE,
                MPI_BYTE,
                Q_rank,
                0,
                MPI_COMM_WORLD,
                &last_msg_to_Q);
        if (msg_i + 1 == expected_messages)
            MPI_Wait(&last_msg_to_Q, &stat);
    }
    free(Q_data);
    free(data);
    close(P_fd);
}
void parity_receiver(Task task)
{
    int16_t coordinator = task.P_rank;
    int Q_fd = open_fileid_new_parity(task.file_id);
    int Q_local_write_error = (Q_fd < 0);
    uint8_t *data = malloc(FILE_TRANSFER_BUFFER_SIZE);
    uint64_t received_from_coordinator = 0;
    uint64_t expected_from_coordinator = task.full_file_size / 2;
    while (received_from_coordinator < expected_from_coordinator)
    {
        receive_sync_message_from(coordinator, FILE_TRANSFER_BUFFER_SIZE, data);
        if (!Q_local_write_error) {
            ssize_t w = write(Q_fd, data, FILE_TRANSFER_BUFFER_SIZE);
            Q_local_write_error |= (w <= 0);
        }
        /* Check for errors */
        received_from_coordinator += FILE_TRANSFER_BUFFER_SIZE;
    }
    close(Q_fd);
    free(data);
    /* If successful - overwrite old parity */
}
void chunk_sender(Task task)
{
    int16_t coordinator = task.P_rank;
    uint64_t data_in_fd = task.full_file_size/2; /* From total size and #sources */
    uint8_t *data = malloc(FILE_TRANSFER_BUFFER_SIZE);
    int have_had_error = 0;
    int fd = open_fileid_readonly(task.file_id);
    if (fd < 0) {
        have_had_error = 1;
    }
    uint64_t read_from_fd = 0;
    while (read_from_fd < data_in_fd)
    {
        if (!have_had_error) {
            ssize_t r = read(fd, data, FILE_TRANSFER_BUFFER_SIZE);
            have_had_error |= (r <= 0);
        }
        read_from_fd += FILE_TRANSFER_BUFFER_SIZE;
        send_sync_message_to(coordinator, FILE_TRANSFER_BUFFER_SIZE, data);
    }
    free(data);
    close(fd);
}

void process_tasklist(int16_t my_rank, int n, const Task tasklist[static n])
{
    for (int i = 0; i < n; i++)
    {
        Task t = tasklist[i];
        int found_in_srcs = 0;
        for (int j = 0; j < MAX_CHUNKS_PER_FILE; j++)
            found_in_srcs |= (t.sources[j] == my_rank);
        if (t.P_rank == my_rank) {
            parity_generator(t);
            time_t current_time = time(NULL);
            printf("Task %d done by rank %d on %s at %s", i, my_rank, hostname, ctime(&current_time));
        }
        else if (t.Q_rank == my_rank)
            parity_receiver(t);
        else if (found_in_srcs)
            chunk_sender(t);
    }
}

static Task generate_random_task(int i)
{
    Task res;
    res.file_id = UINT64_C(0xDEADBEEFDEADBEEF);
    res.full_file_size = UINT64_C(2)*FILE_TRANSFER_BUFFER_SIZE;
    res.full_file_size = FILE_TRANSFER_BUFFER_SIZE + FILE_TRANSFER_BUFFER_SIZE/2;
    int j = (i%2)*4;
    int16_t srcs[] = {1+j,2+j,-1,-1,-1,-1,-1,-1,-1,-1};
    memcpy(res.sources, srcs, sizeof(srcs));
    res.P_rank = 3+j;
    res.Q_rank = 4+j;
    return res;
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_world_size);

    gethostname(hostname, sizeof(hostname));

#if 0
    int meta_data[][9] = {
        {-1,-1,-1,-1,-1,-1,-1,-1},
        {-1,-1,-1,-1,-1,-1,-1,-1},
    };
#endif

    int ntasks = 24;
    Task *tasks = calloc(ntasks, sizeof(Task));
    for (int i = 0; i < ntasks; i++) {
        tasks[i] = generate_random_task(i);
        if (0 && mpi_rank == 0)
            printf("%d: %d, %d, %d, %d\n", i, tasks[i].sources[0], tasks[i].sources[1], tasks[i].P_rank, tasks[i].Q_rank);
    }
    if (mpi_rank > 0)
        process_tasklist(mpi_rank, ntasks, tasks);
    printf("Rank %d done\n", mpi_rank);

    MPI_Finalize();
    return 0;
}
