#ifndef __task_processing__
#define __task_processing__

#include "common.h"
#include "progress_reporting.h"

typedef struct {
    int storage_target;
    ProgressSample *sample;
    /* Potentially corrupt chunks: */
    char *corrupt;
    size_t corrupt_alloc;
    size_t corrupt_bytes_used;
    size_t corrupt_count;
} HostState;

int process_task(
        HostState *hs,
        const char *path,
        const FileInfo *fi,
        TaskInfo ti);

#endif

