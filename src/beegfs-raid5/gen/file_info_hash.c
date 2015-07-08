#include "file_info_hash.h"

#include "khash.h"
KHASH_MAP_INIT_STR(fih, size_t)

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

void fih_add_info(FatFileInfo *fi, int src, int64_t time, int rm)
{
    fi->timestamp = MAX(fi->timestamp, time);
    if (rm)
        fi->deleted |= (1ULL << src);
    else
        fi->modified |= (1ULL << src);
}

int fih_get_or_create(const FileInfoHash *fih, const char *key, size_t *val)
{
    khash_t(fih) *h = fih->h;
    khint_t it = kh_get(fih, h, key);
    if (it != kh_end(h))
    {
        *val = kh_val(h, it);
        return FIH_OLD;
    }
    *val = kh_size(h);
    int r;
    it = kh_put_fih(h, key, &r);
    kh_val(h, it) = *val;
    return FIH_NEW;
}

