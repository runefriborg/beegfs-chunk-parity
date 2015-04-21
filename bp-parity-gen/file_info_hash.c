#include <stdint.h>

#include "file_info_hash.h"

#include "khash.h"
KHASH_MAP_INIT_STR(fih, FileInfo)

struct FileInfoHash {
    khash_t(fih) *h;
};

FileInfoHash* fih_init()
{
    FileInfoHash *res = calloc(1, sizeof(FileInfoHash));
    res->h = kh_init(fih);
    return res;
}

void fih_term(FileInfoHash *fih)
{
    kh_destroy(fih, fih->h);
    memset(fih, 0, sizeof(FileInfoHash));
    free(fih);
}

int fih_add_info(FileInfoHash *fih, char *key, uint16_t src, uint64_t size, uint64_t time)
{
    khash_t(fih) *h = fih->h;
    int r;
    khint_t it = kh_put_fih(h, key, &r);
    FileInfo *fi = &kh_val(h, it);
    if (r == 1)
        memset(fi, 0, sizeof(FileInfo));
    fi->max_chunk_size = MAX(fi->max_chunk_size, size);
    fi->timestamp = time;
    int i = 0;
    for (; fi->locations[i] != 0 && i < 15; i++)
    {
    }
    fi->locations[i] = src;
    return (r == 0);
}

int fih_get(const FileInfoHash *fih, const char *key, FileInfo *val)
{
    khash_t(fih) *h = fih->h;
    khint_t it = kh_get(fih, h, key);
    if (it != kh_end(h))
    {
        *val = kh_val(h, it);
        return 1;
    }
    return 0;
}

