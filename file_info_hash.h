#ifndef __FIH__
#define __FIH__

#include <stdint.h>

#include "khash.h"
#include "common.h"

KHASH_MAP_INIT_STR(fih, FileInfo)


typedef struct {
    khash_t(fih) *h;
} FileInfoHash;

FileInfoHash* fih_init();
int fih_add_info(FileInfoHash *fih, char *key, uint16_t src, uint64_t size, uint64_t time);
int fih_get(const FileInfoHash *fih, const char *key, FileInfo *val);
size_t fih_collect(const FileInfoHash *fih, size_t max, const char **keys, FileInfo *vals);

#endif
