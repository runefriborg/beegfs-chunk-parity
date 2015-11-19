#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
#include <assert.h>
#include <err.h>

#include <leveldb/c.h>

#include "persistent_db.h"

/* Not important what the key is, it just can't collide with a chunkname */
#define FORMAT_VERSION_KEY "?db_version"

struct PersistentDB {
    leveldb_options_t *options;
    leveldb_cache_t *cache;
    leveldb_readoptions_t *ropts;
    leveldb_writeoptions_t *wopts;
    leveldb_t *db;
};

PersistentDB* pdb_init(const char *db_folder, uint64_t expected_version)
{
    leveldb_cache_t *cache = leveldb_cache_create_lru(100*1024*1024);
    leveldb_options_t *db_options = leveldb_options_create();
    leveldb_options_set_create_if_missing(db_options, 1);
    leveldb_options_set_cache(db_options, cache);
    /*
     * Our leveldb is not compiled with snappy support.
     * leveldb_options_set_compression(db_options, leveldb_snappy_compression);
    */
    leveldb_readoptions_t *read_options = leveldb_readoptions_create();
    leveldb_writeoptions_t *write_options = leveldb_writeoptions_create();
    leveldb_writeoptions_set_sync(write_options, 0);
    char *errmsg = NULL;
    leveldb_t *db = leveldb_open(db_options, db_folder, &errmsg);
    if (errmsg != NULL) {
        fprintf(stderr, "%s\n", errmsg);
        return NULL;
    }
    leveldb_free(errmsg);

    PersistentDB *res = calloc(1, sizeof(PersistentDB));
    res->options = db_options;
    res->cache = cache;
    res->wopts = write_options;
    res->ropts = read_options;
    res->db = db;

    size_t version_len;
    uint64_t *version = (uint64_t *)leveldb_get(
            db,
            read_options,
            FORMAT_VERSION_KEY, strlen(FORMAT_VERSION_KEY),
            &version_len,
            &errmsg);
    leveldb_free(errmsg);
    /* New database with no version field */
    if (version == NULL) {
        leveldb_put(
                db,
                write_options,
                FORMAT_VERSION_KEY, strlen(FORMAT_VERSION_KEY),
                (const char *)&expected_version, sizeof(expected_version),
                &errmsg);
        leveldb_free(errmsg);
    }
    else if (version_len != sizeof(*version))
        errx(1, "Corrupt version field in database");
    else if (*version != expected_version)
        errx(1, "Incompatible DB (found: %lu, expected: %lu)",
                *version, expected_version);
    else
        leveldb_free(version);

    return res;
}

void pdb_term(PersistentDB *pdb)
{
    leveldb_close(pdb->db);
}

void pdb_set(PersistentDB *pdb, const char *key, size_t keylen, const FileInfo *val)
{
    char *errmsg = NULL;
    leveldb_put(
            pdb->db,
            pdb->wopts,
            key, keylen,
            (const char *)val, sizeof(FileInfo),
            &errmsg);
    leveldb_free(errmsg);
}

void pdb_del(PersistentDB *pdb, const char *key, size_t keylen)
{
    char *errmsg = NULL;
    leveldb_delete(
            pdb->db,
            pdb->wopts,
            key, keylen,
            &errmsg);
    leveldb_free(errmsg);
}

int pdb_get(const PersistentDB *pdb, const char *key, size_t keylen, FileInfo *val)
{
    size_t fi_len;
    char *errmsg = NULL;
    FileInfo *pfi = (FileInfo *)leveldb_get(
            pdb->db,
            pdb->ropts,
            key, keylen,
            &fi_len,
            &errmsg);
    leveldb_free(errmsg);
    if (pfi == NULL)
        return 0;
    assert(fi_len == sizeof(FileInfo));
    memcpy(val, pfi, fi_len);
    leveldb_free(pfi);
    return 1;
}

void pdb_iterate(const PersistentDB *pdb, ProcessFileInfos f)
{
    int is_done = 0;
    char tmp_key[200];
    leveldb_iterator_t *iter = leveldb_create_iterator(pdb->db, pdb->ropts);
    leveldb_iter_seek_to_first(iter);
    while (!is_done && leveldb_iter_valid(iter)) {
        size_t keylen;
        const char *key = leveldb_iter_key(iter, &keylen);
        memcpy(tmp_key, key, keylen);
        tmp_key[keylen] = '\0';
        size_t vallen;
        const char *val = leveldb_iter_value(iter, &vallen);
        if (strcmp(tmp_key, FORMAT_VERSION_KEY) != 0 && vallen == sizeof(FileInfo))
            is_done = f(tmp_key, keylen, (const FileInfo*)val);
        leveldb_iter_next(iter);
    }
    leveldb_iter_destroy(iter);
}
