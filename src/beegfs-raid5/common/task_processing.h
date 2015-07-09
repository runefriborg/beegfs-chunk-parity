#ifndef __task_processing__
#define __task_processing__

#include "common.h"
#include "progress_reporting.h"

typedef struct {
    int storage_target;
    ProgressSample *sample;
    int corrupt_files_fd;
    int error;
    const char *error_path;
    int fd_null;
    int fd_zero;
    int write_dir;
    int read_chunk_dir;
    int read_parity_dir;
    FILE *log;
} HostState;

int process_task(
        HostState *hs,
        const char *path,
        const FileInfo *fi,
        TaskInfo ti);

#endif

