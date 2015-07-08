#ifndef __FIH__
#define __FIH__

#include <stddef.h>
#include <stdint.h>

#include "../common/common.h"

#define FIH_OLD 0
#define FIH_NEW 1

typedef struct {
    int64_t timestamp;
    uint64_t modified;
    uint64_t deleted;
} FatFileInfo;

typedef struct FileInfoHash FileInfoHash;

FileInfoHash* fih_init();
void fih_term(FileInfoHash *fih);
void fih_add_info(FatFileInfo *fi, int src, int64_t time, int rm);
int fih_get_or_create(const FileInfoHash *fih, const char *key, size_t *val);

#endif
