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

#define FILE_TRANSFER_BUFFER_SIZE (4*1024*1024)
#define PACKETS 500
#define PACKETS_IN_FLIGHT 4

int main(int argc, char **argv)
{
    int mpi_rank;
    char hostname[512];

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    gethostname(hostname, sizeof(hostname));
    printf("%s: %d\n", hostname, mpi_rank);

    char *buffers = calloc(PACKETS_IN_FLIGHT, FILE_TRANSFER_BUFFER_SIZE);
    MPI_Request requests[PACKETS_IN_FLIGHT];
    MPI_Status stats[PACKETS_IN_FLIGHT];
    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    if (mpi_rank == 0) {
        int fd = open("/dev/null", O_WRONLY);
        for (int i = 0; i < PACKETS; i += PACKETS_IN_FLIGHT) {
            if (i > 0) {
                MPI_Waitall(PACKETS_IN_FLIGHT, requests, stats);
            }
            for (int j = 0; j < PACKETS_IN_FLIGHT; j++) {
                MPI_Irecv(
                        buffers + j*FILE_TRANSFER_BUFFER_SIZE,
                        FILE_TRANSFER_BUFFER_SIZE,
                        MPI_BYTE,
                        1,
                        0,
                        MPI_COMM_WORLD,
                        &requests[j]);
            }
        }
        MPI_Waitall(PACKETS_IN_FLIGHT, requests, stats);
        close(fd);
    } else if (mpi_rank == 1) {
        int fd = open("/dev/zero", O_RDONLY);
        for (int i = 0; i < PACKETS; i += PACKETS_IN_FLIGHT) {
            if (i > 0) {
                MPI_Waitall(PACKETS_IN_FLIGHT, requests, stats);
            }
            for (int j = 0; j < PACKETS_IN_FLIGHT; j++) {
                MPI_Isend(
                        buffers + j*FILE_TRANSFER_BUFFER_SIZE,
                        FILE_TRANSFER_BUFFER_SIZE,
                        MPI_BYTE,
                        0,
                        0,
                        MPI_COMM_WORLD,
                        &requests[j]);
            }
        }
        MPI_Waitall(PACKETS_IN_FLIGHT, requests, stats);
        close(fd);
    }
    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    if (mpi_rank == 0) {
        double dt = (1.0*t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec)*1e-9;
        printf("Time: %.2fms\n", dt*1e3);
        printf("BW: %.3fMB/s\n", PACKETS*FILE_TRANSFER_BUFFER_SIZE / dt / 1024 / 1024);
    }
    MPI_Finalize();
    return 0;
}
