#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>

/* "readdir" etc. are defined here. */
#include <dirent.h>

/* limits.h defines "PATH_MAX". */
#include <limits.h>

#define READDIR_THREADS 32
#define QUEUE_SIZE 10000000

#include "mutexqueue.h"

// Global values shared between threads
struct mutexqueue * dir_queue;
int SKIPCHARS;


/* Worker thread for listing dirs */
void * dir_worker (void * fileoutput) {

  DIR * d;
  char * dir_name;
  FILE * fp = fileoutput;
  struct dirent dirent_data; 
  long count;

  count = 0;
 
  while (1) {

    /* Blocking dequeue */
    dir_name = dequeue(dir_queue);

    if (dir_name == NULL) {
      /* The mutexqueue is empty and all threads have completed */
      break;
    }

    /* Open the directory specified by "dir_name". */
    d = opendir (dir_name);

    /* Check it was opened. */
    if (! d) {
        fprintf (stderr, "Cannot open directory '%s': %s\n",
                 dir_name, strerror (errno));
        exit (EXIT_FAILURE);
    }

    /* Loops until no more entries in dir */
    while (1) {
        struct dirent * entry;
        const char * d_name;
	int retval;

        /* "Readdir" gets subsequent entries from "d". */
        retval = readdir_r (d, &dirent_data, &entry);
        if (retval != 0) {
	  fprintf (stderr, "Readdir_r failed");
	  exit(EXIT_FAILURE);
        }
 
        // readdir_r returns NULL in *result if the end 
        // of the directory stream is reached
        if (entry == NULL) {
	  break;
	}

	/* Write the file entry to output file */
        d_name = entry->d_name;
        if (! (entry->d_type & DT_DIR)) {
          if (strlen(dir_name) > SKIPCHARS) {
	    fprintf (fp, "%s/%s\n", dir_name + SKIPCHARS, d_name);
          } else {
	    fprintf (fp, "/%s\n", d_name);
          }

          count++;
          if ((count % 100000) == 0) {
            fprintf(stdout, ".");
            fflush(stdout);
          }
	}

        if (entry->d_type & DT_DIR) {

            /* Check that the directory is not "d" or d's parent. */
            if (strcmp (d_name, "..") != 0 &&
                strcmp (d_name, ".") != 0) {
                int path_length;
                char * path = (char *) malloc(PATH_MAX*sizeof(char));
 
                path_length = snprintf (path, PATH_MAX,
                                        "%s/%s", dir_name, d_name);
                if (path_length >= PATH_MAX) {
                    fprintf (stderr, "Path length has got too long.\n");
                    exit (EXIT_FAILURE);
                }

		/* add path to dir_queue */
		if (enqueue(dir_queue, path) != 0) {
                    fprintf (stderr, "Enqueue failed\n");
		    exit (EXIT_FAILURE);
                }
            }
	}
    }
    /* After going through all the entries, close the directory. */
    if (closedir (d)) {
        fprintf (stderr, "Could not close '%s': %s\n",
                 dir_name, strerror (errno));
        exit (EXIT_FAILURE);
    }
    
    /* Free dequeued element from mutexqueue */
    free(dir_name);
  }
  pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
  pthread_t threads[READDIR_THREADS];
  pthread_attr_t attr;
  FILE *fp[READDIR_THREADS];
  int i;
  char tmp[64];

  /* Use malloc for root element! */
  char * dir = (char *) malloc(PATH_MAX*sizeof(char));
 
  /* Get args */
  if(argc == 3) {
    snprintf (dir, PATH_MAX, "%s", argv[2]);
    dir= realpath(dir, NULL);
    SKIPCHARS=atoi(argv[1]);
  } else {
    printf("Usage: bp-cm-filelist <skipNchars> <directory>\n");
    exit(1);
  }

  fprintf(stdout, ".");
  fflush(stdout);

  /* Allocate mutexqueue and enqueue root element */
  dir_queue= mutexqueue_create(READDIR_THREADS, QUEUE_SIZE);
  enqueue(dir_queue, dir);

  /* Create output files */
  for (i = 0; i<READDIR_THREADS; i++) {
    sprintf(tmp, "output.%d", i);
    fp[i] = fopen(tmp, "w");
  }

  /* For portability, explicitly create threads in a joinable state */
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  for(i = 0; i < READDIR_THREADS; i++) {
    pthread_create(&threads[i], &attr, dir_worker, fp[i]);
  }

  /* Wait for all threads to complete */
  for (i=0; i<READDIR_THREADS; i++) {
    pthread_join(threads[i], NULL);
  }

  /* Close output files */
  for (i = 0; i<READDIR_THREADS; i++) {
    fclose(fp[i]);
  }

  printf("\n");

  /* Clean up and exit */
  pthread_attr_destroy(&attr);
  mutexqueue_destroy(dir_queue);
  pthread_exit(NULL);

  return 0;
}

