#ifndef __task_processing__
#define __task_processing__

#include "common.h"
#include "progress_reporting.h"

typedef struct {
    int storage_target;
    ProgressSample *sample;
    int corrupt_files_fd;
} HostState;

int process_task(
        HostState *hs,
        const char *path,
        const FileInfo *fi,
        TaskInfo ti);

#endif

