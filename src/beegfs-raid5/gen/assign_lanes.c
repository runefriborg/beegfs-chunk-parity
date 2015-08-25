#include <stdlib.h>
#include <memory.h>
#include <assert.h>

#include "assign_lanes.h"

#define PER_LANE 16
#define LANE_MASK 15

#define AS_BITMASK(x) (((x) & L_MASK) | (1 << GET_P(x)))

void assign_lanes(int nlanes, u64 njobs, const FileInfo *jobs, int *lane)
{
    u64 prev_size = nlanes*PER_LANE*sizeof(u64);
    u64 *prev = calloc(1, prev_size);
    int *offsets = calloc(nlanes, sizeof(int));

    for (size_t i = 0; i < njobs; i++)
    {
        u64 target = AS_BITMASK(jobs[i].locations);
        int lane_offset = (i%nlanes);
        int best_idx = lane_offset;
        int best_so_far = 0;
        for (int j0 = 0; j0 < nlanes; j0++)
        {
            int j = (lane_offset + j0) % nlanes;
            int dist = PER_LANE;
            int offset = offsets[j];
            for (int k = 0; k < PER_LANE; k++)
            {
                u64 val = prev[j*PER_LANE + ((offset + k) & LANE_MASK)];
                if (target & val)
                    dist = PER_LANE - k;
            }
            if (dist > best_so_far) {
                best_so_far = dist;
                best_idx = j;
            }
        }
        lane[i] = best_idx;
        prev[best_idx*PER_LANE + offsets[best_idx]] = target;
        offsets[best_idx] = (offsets[best_idx] + 1) & LANE_MASK;
    }
    free(offsets);
    free(prev);
}
