#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
#include <assert.h>

#include <leveldb/c.h>

#include "persistent_db.h"

struct PersistentDB {
    leveldb_options_t *options;
    leveldb_cache_t *cache;
    leveldb_readoptions_t *ropts;
    leveldb_writeoptions_t *wopts;
    leveldb_t *db;
};

PersistentDB* pdb_init()
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
    leveldb_t *db = leveldb_open(db_options, "/tmp/persistent-db", &errmsg);
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
    return res;
}

void pdb_term(PersistentDB *pdb)
{
    leveldb_options_destroy(pdb->options);
    leveldb_cache_destroy(pdb->cache);
    leveldb_readoptions_destroy(pdb->ropts);
    leveldb_writeoptions_destroy(pdb->wopts);
    leveldb_close(pdb->db);
    memset(pdb, 0, sizeof(PersistentDB));
    free(pdb);
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
        if (vallen == sizeof(FileInfo))
            is_done = f(tmp_key, keylen, (const FileInfo*)val);
        leveldb_iter_next(iter);
    }
    leveldb_iter_destroy(iter);
}

