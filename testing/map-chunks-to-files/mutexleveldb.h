#include <leveldb/c.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>


struct mutexleveldb {
  pthread_mutex_t mutex;
  leveldb_t *db;
  leveldb_options_t *options;
  leveldb_readoptions_t *roptions;
  leveldb_writeoptions_t *woptions;
  char *err;
};

struct mutexleveldb *mutexleveldb_create(char * filename) {
  struct mutexleveldb *mdb=malloc(sizeof(struct mutexleveldb));
  mdb->err = NULL;

  /******************************************/
  /* OPEN */
  
  mdb->options = leveldb_options_create();
  leveldb_options_set_create_if_missing(mdb->options, 1);
  mdb->db = leveldb_open(mdb->options, filename, &mdb->err);
  
  if (mdb->err != NULL) {
    fprintf(stderr, "Open fail.\n");
    return NULL;
  }
  
  /* reset error var */
  leveldb_free(mdb->err); mdb->err = NULL;

  /* Init mutex */
  pthread_mutex_init(&mdb->mutex, NULL);
  
  return mdb;
}

int mutexleveldb_write(struct mutexleveldb * mdb, char * key, size_t keylen, char * val, size_t vallen) {
  /******************************************/
  /* WRITE */  
  pthread_mutex_lock(&mdb->mutex);

  mdb->woptions = leveldb_writeoptions_create();
  leveldb_put(mdb->db, mdb->woptions, key, keylen, val, vallen, &mdb->err);

  if (mdb->err != NULL) {
    fprintf(stderr, "Write fail.\n");
    pthread_mutex_unlock(&mdb->mutex);
    return(1);
  }

  leveldb_free(mdb->err); mdb->err = NULL;

  pthread_mutex_unlock(&mdb->mutex);
  return 0;
}

int mutexleveldb_close_and_destroy(struct mutexleveldb * mdb) {

  /******************************************/
  /* CLOSE */
  leveldb_close(mdb->db);
  leveldb_free(mdb->err); mdb->err = NULL;

  pthread_mutex_destroy(&mdb->mutex);
  free(mdb);
  return 0;
}


/*
int main(int argc, char *argv[]) {
  struct mutexleveldb * db;
  db = mutexleveldb_create("testdb");

  mutexleveldb_write(db, "Hello", 5, "World", 5);

  mutexleveldb_close_and_destroy(db);
  return 0;
}
*/
