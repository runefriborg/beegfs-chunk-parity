#ifndef __COMMON_H__
#define __COMMON_H__

#include <stdint.h>

#include "progress_reporting.h"

#define MODIFY_EVENT 'm'
#define UNLINK_EVENT 'd'

#define MAX_STORAGE_TARGETS 56
#define TEST_BIT(x,i) ((x) & (1ULL << (i)))
#define GET_P(loc) ((int)((loc) >> 56))
#define P_MASK UINT64_C(0xFF00000000000000)
#define L_MASK UINT64_C(0x00FFFFFFFFFFFFFF)
#define WITH_P(loc, P) (((loc) & L_MASK) | (((P) << 56) & P_MASK))
#define NO_P UINT64_C(0xFF)

typedef struct {
    int64_t timestamp;
    uint64_t locations;
} FileInfo;

typedef struct {
    int read_dir;
    int is_rebuilding;
    int actual_P_st; /* <- Only valid when rebuilding */
    int tag;
    ProgressSample *sample;
} TaskInfo;

typedef struct { int id, rank; } Target;
typedef struct {
    int ntargets;
    Target targetIDs[MAX_STORAGE_TARGETS];
} RunData;

#define MAX(a,b) ((a) > (b)? (a) : (b))
#define MIN(a,b) ((a) < (b)? (a) : (b))

#endif
