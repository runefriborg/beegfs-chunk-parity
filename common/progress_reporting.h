#ifndef __progress_reporting__
#define __progress_reporting__

#include <stdint.h>
#include <mpi.h>

typedef struct {
    double dt;
    size_t nfiles;
    size_t nbytes;
} ProgressSample;

typedef struct {
    MPI_Request request;
    ProgressSample sample_bufer;
    int needs_wait;
    int host_rank;
} ProgressSender;

/* Send a performance sample to the host. */
void pr_report_progress(ProgressSender *s, ProgressSample sample);

/* Tell the host we are done. */
void pr_report_done(ProgressSender *s);

/* Starts a blocking loop that receives and prints performance data until all
 * clients report that they are done. */
void pr_receive_loop(int clients);

#endif
