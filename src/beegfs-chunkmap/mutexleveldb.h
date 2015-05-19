#include <leveldb/c.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>


struct mutexleveldb {
  pthread_mutex_t mutex;
  leveldb_t *db;
  leveldb_options_t *options;
  leveldb_readoptions_t *roptions;
  leveldb_writeoptions_t *woptions;
  char *err;
  double time;
  long count;
};

struct mutexleveldb *mutexleveldb_create(char * filename) {
  struct mutexleveldb *mdb=malloc(sizeof(struct mutexleveldb));
  struct timeval tv;
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

  /* Init count and time */
  mdb->count = 0;
  gettimeofday(&tv, NULL); 
  mdb->time = tv.tv_sec + tv.tv_usec / 1000000.0;

  /* Init mutex */
  pthread_mutex_init(&mdb->mutex, NULL);
  
  return mdb;
}


int mutexleveldb_write2(int progress, struct mutexleveldb * mdb, char * key, size_t keylen, char * val, size_t vallen) {
  struct timeval tv;
  double t;

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

  mdb->count++;
  if (progress != 0 && (mdb->count % progress) == 0) {
    gettimeofday(&tv, NULL); 
    t = tv.tv_sec + tv.tv_usec / 1000000.0;
    fprintf(stderr, "%12ld files processed at %7.0f files/s\n", mdb->count, ((double) progress) / (t - mdb->time));
    mdb->time = t;
  }

  pthread_mutex_unlock(&mdb->mutex);
  return 0;
}

inline int mutexleveldb_write(struct mutexleveldb * mdb, char * key, size_t keylen, char * val, size_t vallen) {
  return mutexleveldb_write2(0, mdb, key, keylen, val, vallen);
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
