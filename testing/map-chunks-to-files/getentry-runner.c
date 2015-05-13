#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>

/* limits.h defines "PATH_MAX". */
#include <limits.h>

#include "mutexleveldb.h"

#define GETENTRY_THREADS 4
#define GETENTRY_COMMAND   "cat output.%d | fhgfs-ctl --getentryinfo --nomappings --unmounted -"
#define PATH_IDENTIFIER    "Path: "
#define ENTRYID_IDENTIFIER "EntryID: "
#define LINE_BUFSIZE PATH_MAX

// Global values shared between threads
struct mutexleveldb * db;

struct arg_struct {
  int thread_id;
};

/* Worker thread for fetching entries */
void * getentry_worker (void * x) {

  char line[LINE_BUFSIZE];
  int linenr;
  FILE *pipe;
  char cmd[PATH_MAX];
  struct arg_struct *args = (struct arg_struct *) x;

  size_t path_identifier_len = strlen(PATH_IDENTIFIER);
  size_t entryid_identifier_len = strlen(ENTRYID_IDENTIFIER);

  char path_line[PATH_MAX];
  char entryid_line[64];

  /* Create cmd */
  sprintf(cmd, GETENTRY_COMMAND, args->thread_id);

  /* Get a pipe where the output from the scripts comes in */
  pipe = popen(cmd, "r");
  if (pipe == NULL) {  /* check for errors */
    fprintf (stderr, "Failed to run: %s\n", cmd); /* report error message */
    exit (EXIT_FAILURE);        /* return with exit code indicating error */
  }

  /* Read script output from the pipe line by line */
  linenr = 1;
  while (fgets(line, LINE_BUFSIZE, pipe) != NULL) {
    if (strncmp(line, PATH_IDENTIFIER, path_identifier_len) == 0) {
      strcpy(path_line, line + (int) path_identifier_len);
      
      // Remove newline
      path_line[(int)strlen(path_line)-1] = '\0';	
    } else if (strncmp(line, ENTRYID_IDENTIFIER, entryid_identifier_len) == 0) {
      strcpy(entryid_line, line + (int) entryid_identifier_len);

      // Remove newline
      entryid_line[(int)strlen(entryid_line)-1] = '\0';

      // Write to db
      mutexleveldb_write(db, entryid_line, strlen(entryid_line), path_line, strlen(path_line));
    }
    ++linenr;
  }
    
  /* Once here, out of the loop, the script has ended. */
  pclose(pipe); /* Close the pipe */

  pthread_exit(NULL);
}


int main(int argc, char *argv[]) {
  pthread_t threads[GETENTRY_THREADS];
  pthread_attr_t attr;
  struct arg_struct thread_args[GETENTRY_THREADS];
  int i;

  /* Get args */
  if(argc == 2) {
    /* Init db */
    db = mutexleveldb_create(argv[1]);
  } else {
    printf("Usage: getentry-runner <leveldb filename>\n");
    exit(1);
  }
 

  /* For portability, explicitly create threads in a joinable state */
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  for(i = 0; i < GETENTRY_THREADS; i++) {
    thread_args[i].thread_id = i;
    pthread_create(&threads[i], &attr, getentry_worker, (void *) &thread_args[i]);
  }

  /* Wait for all threads to complete */
  for (i=0; i<GETENTRY_THREADS; i++) {
    pthread_join(threads[i], NULL);
  }

  /* Clean up and exit */
  pthread_attr_destroy(&attr);
  mutexleveldb_close_and_destroy(db);
  pthread_exit(NULL);

  return 0;
}

