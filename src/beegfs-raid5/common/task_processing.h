#ifndef __task_processing__
#define __task_processing__

#include "common.h"
#include "progress_reporting.h"

int process_task(
        int my_st,
        const char *path,
        const FileInfo *fi,
        TaskInfo ti,
        ProgressSample *sample);

#endif

