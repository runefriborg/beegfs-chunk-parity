#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>

/* limits.h defines "PATH_MAX". */
#include <limits.h>

#define GETENTRY_THREADS 1
#define GETENTRY_COMMAND   "cat output.%d | fhgfs-ctl --getentryinfo --nomappings --unmounted -"
#define PATH_IDENTIFIER    "Path: "
#define ENTRYID_IDENTIFIER "EntryID: "
#define LINE_BUFSIZE PATH_MAX

// Global values shared between threads
pthread_mutex_t _mutex;

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
    printf("Script output line %d: %s", linenr, line);
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
 
  /* Init mutex */
  pthread_mutex_init(&_mutex, NULL);

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
  pthread_mutex_destroy(&_mutex);
  pthread_exit(NULL);

  return 0;
}

