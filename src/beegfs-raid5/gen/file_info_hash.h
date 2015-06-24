#ifndef __FIH__
#define __FIH__

#include <stdint.h>

#include "../common/common.h"

typedef struct {
    int64_t timestamp;
    uint64_t modified;
    uint64_t deleted;
} FatFileInfo;

typedef struct FileInfoHash FileInfoHash;

FileInfoHash* fih_init();
void fih_term(FileInfoHash *fih);
int fih_add_info(FileInfoHash *fih, char *key, int src, int64_t time, int rm);
int fih_get(const FileInfoHash *fih, const char *key, FatFileInfo *val);

#endif
