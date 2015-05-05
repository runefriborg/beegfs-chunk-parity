#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>
/* "readdir" etc. are defined here. */
#include <dirent.h>
/* limits.h defines "PATH_MAX". */
#include <limits.h>

#define READDIR_THREADS 8

#include "mutexqueue.h"

struct circqueue * dirqueue;

/* List the files in "dir_name". */
static void list_dir () {
  DIR * d;
  char * dir_name;

  while (1) {
        
    dir_name = dequeue(dirqueue);

    if (dir_name == NULL) {
      break;
    }

    dir_name = dequeue(dirqueue);
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
	    printf ("%s/%s\n", dir_name, d_name);
	}

        if (entry->d_type & DT_DIR) {

            /* Check that the directory is not "d" or d's parent. */
            
            if (strcmp (d_name, "..") != 0 &&
                strcmp (d_name, ".") != 0) {
                int path_length;
                char * path = (char *) malloc(PATH_MAX*sizeof(char));
 
                path_length = snprintf (path, PATH_MAX,
                                        "%s/%s", dir_name, d_name);
                printf ("%s\n", path);
                if (path_length >= PATH_MAX) {
                    fprintf (stderr, "Path length has got too long.\n");
                    exit (EXIT_FAILURE);
                }

		/* add path to dirqueue */
                /* Recursively call "list_dir" with the new path. */
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
}

int main ()
{
  char * dir = (char *) malloc(PATH_MAX*sizeof(char));
  snprintf (dir, PATH_MAX, "%s", "/faststorage/home/runef");
  
  dirqueue= mutexqueue(READDIR_THREADS, 1000);
  enqueue(dirqueue, dir);
  list_dir ();
  return 0;
}

