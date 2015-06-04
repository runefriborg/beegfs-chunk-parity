#ifndef __progress_reporting__
#define __progress_reporting__

#include <stdint.h>
#include <mpi.h>

/* The first part of the sample is temporary variables that should be reset
 * every time progress is reported. The second half is total values for the
 * entire run. */
typedef struct {
    double dt;
    size_t nfiles;
    size_t bytes_read;
    size_t bytes_written;

    double total_time;
    size_t total_nfiles;
    size_t total_bytes_read;
    size_t total_bytes_written;
} ProgressSample;

#define PROGRESS_SAMPLE_INIT {0.0, 0, 0, 0, 0.0, 0, 0, 0}

typedef struct {
    MPI_Request request;
    ProgressSample sample_bufer;
    int needs_wait;
    int host_rank;
} ProgressSender;

/* Increment the total values by the temp values. */
void pr_add_tmp_to_total(ProgressSample *sample);

/* Clear out the temp values. */
void pr_clear_tmp(ProgressSample *sample);

/* Send a performance sample to the host. */
void pr_report_progress(ProgressSender *s, ProgressSample sample);

/* Tell the host we are done. */
void pr_report_done(ProgressSender *s);

/* Starts a blocking loop that receives and prints performance data until all
 * clients report that they are done. */
void pr_receive_loop(int clients);

#endif
