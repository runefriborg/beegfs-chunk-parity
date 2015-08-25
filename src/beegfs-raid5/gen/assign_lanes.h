#ifndef __ASSIGN_LANES__
#define __ASSIGN_LANES__

#include "../common/common.h"

typedef unsigned long long u64;
void assign_lanes(int nlanes, u64 njobs, const FileInfo *jobs, int *lane);

#endif
