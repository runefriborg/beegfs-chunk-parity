#ifndef __pdb__
#define __pdb__

#include <stdint.h>

#include "common.h"

typedef struct PersistentDB PersistentDB;

PersistentDB* pdb_init();
void pdb_term(PersistentDB *pdb);
void pdb_set(PersistentDB *pdb, const char *key, size_t keylen, const FileInfo *val);
int pdb_get(const PersistentDB *pdb, const char *key, FileInfo *val);

#endif
