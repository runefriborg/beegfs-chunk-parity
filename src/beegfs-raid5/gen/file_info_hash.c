#include <stdint.h>

#include "file_info_hash.h"

#include "khash.h"
KHASH_MAP_INIT_STR(fih, FatFileInfo)

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

int fih_add_info(FileInfoHash *fih, char *key, int src, uint64_t time, int rm)
{
    khash_t(fih) *h = fih->h;
    int r;
    khint_t it = kh_put_fih(h, key, &r);
    FatFileInfo *fi = &kh_val(h, it);
    if (r == 1)
        memset(fi, 0, sizeof(FatFileInfo));
    fi->timestamp = time;
    if (rm)
        fi->deleted |= (1ULL << src);
    else
        fi->modified |= (1ULL << src);
    return (r == 0);
}

int fih_get(const FileInfoHash *fih, const char *key, FatFileInfo *val)
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

