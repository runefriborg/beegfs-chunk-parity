#include <stdio.h>
#include <memory.h>

#include "common.h"
#include "progress_reporting.h"

void pr_report_progress(ProgressSender *s, ProgressSample sample)
{
    if (sample.nfiles == 0 || sample.nbytes == 0)
        return;

    if (s->needs_wait) {
        MPI_Status stat;
        MPI_Wait(&s->request, &stat);
        s->needs_wait = 0;
    }

    s->sample_bufer = sample;
    MPI_Isend(
            &s->sample_bufer,
            sizeof(ProgressSample),
            MPI_BYTE,
            s->host_rank,
            0,
            MPI_COMM_WORLD,
            &s->request);
    s->needs_wait = 1;
}

void pr_report_done(ProgressSender *s)
{
    ProgressSample sample = {0.0, ~0, ~0};
    pr_report_progress(s, sample);
}

void pr_receive_loop(int clients)
{
    int remaining_clients = clients;
    while (remaining_clients > 0)
    {
        MPI_Status stat;
        memset(&stat, 0, sizeof(stat));
        ProgressSample sample;
        MPI_Recv(&sample, sizeof(sample), MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &stat);
        if (sample.nfiles == (size_t)~0) {
            remaining_clients -= 1;
        }
        else {
            printf("%2d - %7zu files | %9.2f MiB/s | %9.0f files/s\n",
                    stat.MPI_SOURCE,
                    sample.nfiles,
                    ((double)sample.nbytes / 1024 / 1024) / sample.dt,
                    sample.nfiles / sample.dt);
            fflush(stdout);
        }
    }
}
