#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>

/* limits.h defines "PATH_MAX". */
#include <limits.h>

#include "perf.h"

#undef PROFILE
#define GETENTRY_THREADS 32
#define GETENTRY_COMMAND   "cat output.%d | beegfs-ctl --getentryinfo --unmounted -"
#define PATH_IDENTIFIER    "Path: "
#define LINE_BUFSIZE PATH_MAX

// GLOBALS
long FILECOUNT;
char TARGETID[128];
char MOUNTPOINT[PATH_MAX];

struct arg_struct {
  int thread_id;
  int thread_count;
};

/* Worker thread for fetching entries */
void * getentry_worker (void * x) {

  char line[LINE_BUFSIZE];
  long linenr;
  FILE *pipe;
  char cmd[PATH_MAX];
  struct arg_struct *args = (struct arg_struct *) x;

  #ifdef PROFILE
    perf_entry_t * perf_getentry, * perf_strcmp;
  #endif

  size_t path_identifier_len = strlen(PATH_IDENTIFIER);

  char path_line[PATH_MAX];

  #ifdef PROFILE
    perf_getentry = perf_create("thread", args->thread_id*3+1, 0);
    perf_strcmp   = perf_create("strcmp", args->thread_id*3+3, args->thread_id*3+1);
  
    perf_update_start(perf_getentry);
  #endif

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
    #ifdef PROFILE
      perf_update_start(perf_strcmp);  
    #endif
    if (strncmp(line, PATH_IDENTIFIER, path_identifier_len) == 0) {

      strcpy(path_line, line + (int) path_identifier_len);
      
      // Remove newline
      path_line[(int)strlen(path_line)-1] = '\0';	

    } else if (strstr(line, TARGETID) != NULL) { 
      /* TARGETID found. Rename to .CORRUPT */

      
      if (strstr(path_line, ".CORRUPT") == NULL) {
	char filename0[PATH_MAX];
	char filename_fixed[PATH_MAX];
	snprintf(filename0, sizeof(filename0), "%s%s", MOUNTPOINT, path_line);
	snprintf(filename_fixed, sizeof(filename_fixed), "%s.CORRUPT", filename0);
	rename(filename0, filename_fixed);
	printf("%s\n", filename_fixed);
      }
    }
    #ifdef PROFILE
      perf_update_tick(perf_strcmp);
    #endif
    ++linenr;
    
  }
    
  /* Once here, out of the loop, the script has ended. */
  pclose(pipe); /* Close the pipe */

  #ifdef PROFILE
    perf_update_tick(perf_getentry);

    perf_submit(perf_getentry);
    perf_submit(perf_strcmp);
  #endif

  pthread_exit(NULL);
  
}


int main(int argc, char *argv[]) {
  pthread_t threads[GETENTRY_THREADS];
  pthread_attr_t attr;
  struct arg_struct thread_args[GETENTRY_THREADS];
  int i;

  #ifdef PROFILE
    perf_entry_t * perf_main;
  #endif

  /* Get args */
  if(argc == 4) {
    snprintf (MOUNTPOINT, PATH_MAX, "%s", argv[1]);
    FILECOUNT = atoi(argv[2]);
    snprintf (TARGETID, 128, "@ %s", argv[3]);
  } else {
    printf("Usage: bp-cm-getentry <BeeGFS mountpoint> <filecount> <target id>\n");
    exit(1);
  }
 
  #ifdef PROFILE
    perf_global_init();
    perf_main = perf_create("Main", 0, -1);
    perf_update_start(perf_main);
  #endif

  /* For portability, explicitly create threads in a joinable state */
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  for(i = 0; i < GETENTRY_THREADS; i++) {
    thread_args[i].thread_id = i;
    thread_args[i].thread_count = GETENTRY_THREADS;

    pthread_create(&threads[i], &attr, getentry_worker, (void *) &thread_args[i]);
  }

  /* Wait for all threads to complete */
  for (i=0; i<GETENTRY_THREADS; i++) {
    pthread_join(threads[i], NULL);
  }

  #ifdef PROFILE
    perf_update_tick(perf_main);
    perf_submit(perf_main);
    perf_output_report(0);
    perf_global_free();
  #endif

  /* Clean up and exit */
  pthread_attr_destroy(&attr);

  return 0;
}
