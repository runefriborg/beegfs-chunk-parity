#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>
/* "readdir" etc. are defined here. */
#include <dirent.h>
/* limits.h defines "PATH_MAX". */
#include <limits.h>

#define READDIR_THREADS 4

#include "mutexqueue.h"

struct circqueue * dirqueue;

/* List the files in "dir_name". */
void * list_dir (void * x) {
  DIR * d;
  char * dir_name;

  while (1) {        
    dir_name = dequeue(dirqueue);

    if (dir_name == NULL) {
      printf("B");
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
    while (1) {
        struct dirent * entry;
        const char * d_name;

        /* "Readdir" gets subsequent entries from "d". */
        entry = readdir (d);
        if (! entry) {
            /* There are no more entries in this directory, so break
               out of the while loop. */
            break;
        }
        d_name = entry->d_name;
        if (! (entry->d_type & DT_DIR)) {
	  //printf ("%s/%s\n", dir_name, d_name);
	  printf(".");
	}

        if (entry->d_type & DT_DIR) {

            /* Check that the directory is not "d" or d's parent. */
            
            if (strcmp (d_name, "..") != 0 &&
                strcmp (d_name, ".") != 0) {
                int path_length;
                char * path = (char *) malloc(PATH_MAX*sizeof(char));
 
                path_length = snprintf (path, PATH_MAX,
                                        "%s/%s", dir_name, d_name);
                //printf ("%s\n", path);
		printf(".");
                if (path_length >= PATH_MAX) {
                    fprintf (stderr, "Path length has got too long.\n");
                    exit (EXIT_FAILURE);
                }

		/* add path to dirqueue */
		enqueue(dirqueue, path);
            }
	}
    }
    /* After going through all the entries, close the directory. */
    if (closedir (d)) {
        fprintf (stderr, "Could not close '%s': %s\n",
                 dir_name, strerror (errno));
        exit (EXIT_FAILURE);
    }
    
    free(dir_name);
  }
  pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
  pthread_t threads[READDIR_THREADS];
  pthread_attr_t attr;
  int i;
  
  char * dir = (char *) malloc(PATH_MAX*sizeof(char));
 
  if(argc == 1) {
    snprintf (dir, PATH_MAX, "%s", ".");
  } else {
    snprintf (dir, PATH_MAX, "%s", argv[1]);
  }

  dirqueue= mutexqueue(READDIR_THREADS, 100000);
  enqueue(dirqueue, dir);

  /* For portability, explicitly create threads in a joinable state */
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  for(i = 0; i < READDIR_THREADS; i++) {
    pthread_create(&threads[i], &attr, list_dir, 0);
  }

  /* Wait for all threads to complete */
  for (i=0; i<READDIR_THREADS; i++) {
    pthread_join(threads[i], NULL);
  }
  /* Clean up and exit */
  pthread_attr_destroy(&attr);
  mutexqueue_destroy(dirqueue);
  pthread_exit(NULL);

  return 0;
}

