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
    PersistentDB *res = calloc(1, sizeof(PersistentDB));
    if (errmsg != NULL) {
        fprintf(stderr, "%s\n", errmsg);
    }
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
}

int pdb_get(const PersistentDB *pdb, const char *key, size_t keylen, FileInfo *val)
{
    size_t fi_len;
    char *errmsg;
    FileInfo *pfi = (FileInfo *)leveldb_get(
            pdb->db,
            pdb->ropts,
            key, keylen,
            &fi_len,
            &errmsg);
    if (pfi == NULL)
        return 0;
    assert(fi_len == sizeof(FileInfo));
    memcpy(val, pfi, fi_len);
    leveldb_free(pfi);
    return 1;
}

