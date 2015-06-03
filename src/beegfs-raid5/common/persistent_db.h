#ifndef __pdb__
#define __pdb__

#include <stdint.h>

#include "common.h"

typedef struct PersistentDB PersistentDB;
typedef int (*ProcessFileInfos)(const char *key, size_t keylen, const FileInfo* info);

PersistentDB* pdb_init();
void pdb_term(PersistentDB *pdb);
void pdb_set(PersistentDB *pdb, const char *key, size_t keylen, const FileInfo *val);
int pdb_get(const PersistentDB *pdb, const char *key, size_t keylen, FileInfo *val);
void pdb_iterate(const PersistentDB *pdb, ProcessFileInfos f);

#endif
