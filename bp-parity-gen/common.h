#ifndef __COMMON_H__
#define __COMMON_H__

#include <stdint.h>

#define MAX_LOCS 15
#define P_INDEX (MAX_LOCS+0)

typedef struct {
    uint64_t max_chunk_size;
    uint64_t timestamp;
    int16_t locations[MAX_LOCS + 1];
} FileInfo;

#define MAX(a,b) ((a) > (b)? (a) : (b))
#define MIN(a,b) ((a) < (b)? (a) : (b))

#endif
