#ifndef __COMMON_H__
#define __COMMON_H__

#include <stdint.h>

#define MAX_LOCS 14
#define P_INDEX (MAX_LOCS+0)
#define Q_INDEX (MAX_LOCS+1)

typedef struct {
    uint64_t full_file_size;
    uint64_t byte_size;
    uint64_t timestamp;
    int16_t locations[MAX_LOCS + 2];
} FileInfo;

#define MAX(a,b) ((a) > (b)? (a) : (b))

#endif
