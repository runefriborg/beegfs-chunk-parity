#include <assert.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include <mpi.h>

#include "../common/common.h"
#include "../common/progress_reporting.h"
#include "../common/task_processing.h"
#include "../common/persistent_db.h"

#define PROF_START(name) \
    struct timespec t_##name##_0; \
    clock_gettime(CLOCK_MONOTONIC, &t_##name##_0)
#define PROF_END(name) \
    struct timespec t_##name##_1; \
    clock_gettime(CLOCK_MONOTONIC, &t_##name##_1)
#define PROF_VAL(name) \
    ((t_##name##_1.tv_sec - t_##name##_0.tv_sec) * 1.0 \
    + (t_##name##_1.tv_nsec - t_##name##_0.tv_nsec) * 1e-9)

static int rebuild_target;
static int helper;
static int mpi_rank;
static int mpi_world_size;
int st2rank[MAX_STORAGE_TARGETS];
int rank2st[MAX_STORAGE_TARGETS+1];

static ProgressSender pr_sender;
static ProgressSample pr_sample = {0.0, 0, 0};


int do_file(const char *key, size_t keylen, const FileInfo *fi)
{
    struct timespec tv1;
    clock_gettime(CLOCK_MONOTONIC, &tv1);

    int my_st = rank2st[mpi_rank];
    int P = GET_P(fi->locations);
    if (P == NO_P)
        goto done;
    if (P != rebuild_target && TEST_BIT(fi->locations, rebuild_target) == 0)
        goto done; /* rebuild target is not involved in this item */
    if (helper == my_st) {
        /* Send fi, key to rebuild_target so it knows whats up */
        MPI_Ssend((void*)fi, sizeof(FileInfo), MPI_BYTE, st2rank[rebuild_target], 0, MPI_COMM_WORLD);
        MPI_Ssend((void*)key, keylen, MPI_BYTE, st2rank[rebuild_target], 0, MPI_COMM_WORLD);
    }
    /* The rank that holds the P block needs read from parity in stead of
     * chunks, and the rebuild target has to write to chunks rather than
     * parity. If the rebuild target had the original P block, we don't want to
     * switch - we just want to recalculate it. */
    const char *load_pat = "         chunks";
    const char *save_pat = "         parity";
    if ((P != rebuild_target) && (P == my_st || rebuild_target == my_st)) {
        const char *tmp = load_pat;
        load_pat = save_pat;
        save_pat = tmp;
    }
    FileInfo mod_fi = *fi;
    if (P != rebuild_target) {
        mod_fi.locations |= (1 << P);
        mod_fi.locations &= ~(1 << rebuild_target);
        mod_fi.locations = WITH_P(mod_fi.locations, (uint64_t)rebuild_target);
    }
    TaskInfo ti = { load_pat, save_pat, (P != rebuild_target), P };
    int report = process_task(my_st, key, &mod_fi, ti, &pr_sample.nbytes);
#if 0
#define FIRST_8_BITS(x)     ((x) & 0x80 ? 1 : 0), ((x) & 0x40 ? 1 : 0), \
      ((x) & 0x20 ? 1 : 0), ((x) & 0x10 ? 1 : 0), ((x) & 0x08 ? 1 : 0), \
      ((x) & 0x04 ? 1 : 0), ((x) & 0x02 ? 1 : 0), ((x) & 0x01 ? 1 : 0) 
    int locs = mod_fi.locations & 0xFF;
    int cP = GET_P(mod_fi.locations);
    if (rank2st[mpi_rank] == rebuild_target)
    printf("process_task(%d, '%s', %d%d%d%d%d%d%d%d, op=%d, np=%d, '%s', '%s')\n", my_st, key, FIRST_8_BITS(locs), P, cP, load_pat, save_pat);
#endif

    struct timespec tv2;
    clock_gettime(CLOCK_MONOTONIC, &tv2);
    double dt = (tv2.tv_sec - tv1.tv_sec) * 1.0
        + (tv2.tv_nsec - tv1.tv_nsec) * 1e-9;
    if (report) {
        pr_sample.dt += dt;
        pr_sample.nfiles += 1;
    }
    if (pr_sample.dt >= 1.0) {
        pr_report_progress(&pr_sender, pr_sample);
        memset(&pr_sample, 0, sizeof(pr_sample));
    }
done:
    return 0;
}

int main(int argc, char **argv)
{
    if (argc != 4)
    {
        fputs("We need 3 arguments\n", stdout);
        return 1;
    }

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_world_size);

    rebuild_target = atoi(argv[1]);
    const char *store_dir = argv[2];
    const char *data_file = argv[3];

    int ntargets = mpi_world_size - 1;
    if (ntargets > MAX_STORAGE_TARGETS)
        return 1;

    if (rebuild_target < 0 || rebuild_target > ntargets)
        return 1;
    helper = 1;
    while (helper == rebuild_target)
        helper += 1;
    if (helper == rebuild_target)
        return 1;

    PROF_START(total);
    PROF_START(init);

    int last_run_fd = -1;
    RunData last_run;
    memset(&last_run, 0, sizeof(RunData));
    if (mpi_rank == 0) {
        last_run_fd = open(data_file, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        read(last_run_fd, &last_run, sizeof(RunData));
    }

    /* Create mapping from storage targets to ranks, and vice versa */
    Target targetIDs[MAX_STORAGE_TARGETS] = {{0,0}};
    Target targetID = {0,0};
    if (mpi_rank != 0)
    {
        int store_fd = open(store_dir, O_DIRECTORY | O_RDONLY);
        int target_ID_fd = openat(store_fd, "targetNumID", O_RDONLY);
        char targetID_s[20] = {0};
        read(target_ID_fd, targetID_s, sizeof(targetID_s));
        close(target_ID_fd);
        close(store_fd);
        targetID.id = atoi(targetID_s);
        targetID.rank = mpi_rank;
    }
    MPI_Gather(
            &targetID, sizeof(Target), MPI_BYTE,
            targetIDs, sizeof(Target), MPI_BYTE,
            0,
            MPI_COMM_WORLD);
    if (mpi_rank == 0) {
        if (last_run.ntargets != ntargets) {
            /* ERROR - new number of targets */
            assert(0);
        }
        for (int i = 0; i < ntargets; i++)
            targetIDs[i] = targetIDs[i+1];
        for (int i = 0; i < ntargets; i++)
            last_run.targetIDs[i].rank = -1;
        for (int i = 0; i < ntargets; i++) {
            Target target = targetIDs[i];
            int j = 0;
            int found = 0;
            for (; j < ntargets; j++)
                if (last_run.targetIDs[j].id == target.id) {
                    last_run.targetIDs[j] = target;
                    found = 1;
                }
            if (!found) {
                /* ERROR - new target introduced */
                printf(" > %d, %d\n", target.id, target.rank);
                assert(0);
            }
        }
        rank2st[0] = -1;
        for (int i = 0; i < ntargets; i++)
        {
            st2rank[i] = last_run.targetIDs[i].rank;
            rank2st[st2rank[i]] = i;
        }
    }
    MPI_Bcast(st2rank, sizeof(st2rank), MPI_BYTE, 0, MPI_COMM_WORLD);
    MPI_Bcast(rank2st, sizeof(rank2st), MPI_BYTE, 0, MPI_COMM_WORLD);

    PROF_END(init);

    if (mpi_rank == 0)
        printf("%d(rank=%d), %d(rank=%d)\n", rebuild_target, st2rank[rebuild_target], helper, st2rank[helper]);

    PROF_START(main_work);

    memset(&pr_sender, 0, sizeof(pr_sender));

    if (mpi_rank != 0 && rank2st[mpi_rank] != rebuild_target)
    {
        PersistentDB *pdb = pdb_init();
        pdb_iterate(pdb, do_file);
        pdb_term(pdb);

        if (rank2st[mpi_rank] == helper) {
            int dummy;
            MPI_Ssend((void*)&dummy, sizeof(dummy), MPI_BYTE, st2rank[rebuild_target], 0, MPI_COMM_WORLD);
        }
        pr_report_progress(&pr_sender, pr_sample);
        pr_report_done(&pr_sender);
    }
    else if (rank2st[mpi_rank] == rebuild_target)
    {
        int helper_rank = st2rank[helper];
        MPI_Status stat;
        int count;
        FileInfo fi;
        MPI_Recv(&fi, sizeof(FileInfo), MPI_BYTE, helper_rank, 0, MPI_COMM_WORLD, &stat);
        MPI_Get_count(&stat, MPI_BYTE, &count);
        while (count == sizeof(FileInfo)) {
            char key[200];
            MPI_Recv(key, sizeof(key), MPI_BYTE, helper_rank, 0, MPI_COMM_WORLD, &stat);
            int keylen;
            MPI_Get_count(&stat, MPI_BYTE, &keylen);
            key[keylen] = '\0';
            do_file(key, keylen, &fi);

            MPI_Recv(&fi, sizeof(FileInfo), MPI_BYTE, helper_rank, 0, MPI_COMM_WORLD, &stat);
            MPI_Get_count(&stat, MPI_BYTE, &count);
        }
        pr_report_progress(&pr_sender, pr_sample);
        pr_report_done(&pr_sender);
    }
    else if (mpi_rank == 0)
    {
        pr_receive_loop(ntargets-1);
    }

    PROF_END(main_work);
    PROF_END(total);

    if (mpi_rank == 0) {
        printf("Overall timings: \n");
        printf("init       | %9.2f ms\n", 1e3*PROF_VAL(init));
        printf("main_work  | %9.2f ms\n", 1e3*PROF_VAL(main_work));
        printf("total      | %9.2f ms\n", 1e3*PROF_VAL(total));
    }

    MPI_Finalize();
}
