#ifndef __FIH__
#define __FIH__

#include <stdint.h>

#include "../common/common.h"

typedef struct FileInfoHash FileInfoHash;

FileInfoHash* fih_init();
void fih_term(FileInfoHash *fih);
int fih_add_info(FileInfoHash *fih, char *key, int src, uint64_t time);
int fih_get(const FileInfoHash *fih, const char *key, FileInfo *val);

#endif
