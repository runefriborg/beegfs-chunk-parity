#ifndef __COMMON_H__
#define __COMMON_H__

#include <stdint.h>

#define MODIFY_EVENT 0
#define UNLINK_EVENT 1

#define MAX_STORAGE_TARGETS 56
#define TEST_BIT(x,i) ((x) & (1ULL << (i)))
#define GET_P(loc) ((int)((loc) >> 56))
#define P_MASK UINT64_C(0xFF00000000000000)
#define L_MASK UINT64_C(0x00FFFFFFFFFFFFFF)
#define WITH_P(loc, P) (((loc) & L_MASK) | (((P) << 56) & P_MASK))
#define NO_P UINT64_C(0xFF)

typedef struct {
    uint64_t timestamp;
    uint64_t locations;
} FileInfo;

typedef struct {
    const char *load_pat;
    const char *save_pat;
    int is_rebuilding;
    int actual_P_st; /* <- Only valid when rebuilding */
} TaskInfo;

typedef struct { int id, rank; } Target;
typedef struct {
    int ntargets;
    Target targetIDs[MAX_STORAGE_TARGETS];
} RunData;

#define MAX(a,b) ((a) > (b)? (a) : (b))
#define MIN(a,b) ((a) < (b)? (a) : (b))

#endif
