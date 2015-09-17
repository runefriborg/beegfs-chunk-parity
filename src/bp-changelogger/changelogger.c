#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>

#include <sys/time.h>
#include <sys/stat.h>
#include <errno.h>
#include <dlfcn.h>
#include <stdarg.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <limits.h>


#define MAX_OPEN_FILES 65535
#define MAX_PATH_LENGTH 512
#define CHANGELOGROTATE 86400
#define CHANGELOGFOLDER      "/dev/shm/beegfs-changelog/"
#define LOG                  "/var/log/beegfs-changelog/intercept.log"
#define ERRORLOG             "/var/log/beegfs-changelog/error.log"
#define LOGDIR               "/var/log/beegfs-changelog"


#define DEBUG true


/* Operations for the `flock' call.  */
#define	LOCK_SH	1	/* Shared lock.  */
#define	LOCK_EX	2 	/* Exclusive lock.  */
#define	LOCK_UN	8	/* Unlock.  */

/* Apply or remove an advisory lock, according to OPERATION,
   on the file FD refers to.  */
extern int flock (int __fd, int __operation);


char *open_files[MAX_OPEN_FILES];
char pathbuf[MAX_OPEN_FILES*MAX_PATH_LENGTH];
char *dirpath = NULL;
char *storage_id = NULL;
__thread FILE *log_fd = NULL;
__thread time_t log_create_t = 0;
__thread char *rand_log_name = NULL;
pthread_mutex_t lock;

inline void initlog() {
  int ret = mkdir(LOGDIR, S_IRUSR | S_IWUSR | S_IXUSR);
  if(ret != 0 && errno != EEXIST) {
    fprintf(stderr, "Could not create logdir '%s'.\n", LOGDIR);
    return;
  }
}

inline void errorlog(const char *format,...) {
  FILE *f;
  char logfilename[MAX_PATH_LENGTH];
  pthread_mutex_lock(&lock);

  if (storage_id == NULL) {
    f = fopen(ERRORLOG,"a");
  } else {
    snprintf(logfilename, MAX_PATH_LENGTH,"%s%s",ERRORLOG,storage_id);
    f = fopen(logfilename,"a");
  }

  if (f != NULL) {
    struct timeval tv;
    gettimeofday(&tv,NULL);

    // write microtimestamp
    fprintf(f, "%lu.%lu ",(unsigned long)tv.tv_sec,(unsigned long)tv.tv_usec);
    
    // write message
    va_list args;
    va_start( args, format );
    vfprintf( f, format, args );
    va_end( args );
    fclose(f);
  }
  pthread_mutex_unlock(&lock);
}

inline void writelog(const char *format,...) {
  FILE *f;
  char logfilename[MAX_PATH_LENGTH];
  pthread_mutex_lock(&lock);

  if (storage_id == NULL) {
    f = fopen(LOG,"a");
  } else {
    snprintf(logfilename, MAX_PATH_LENGTH,"%s%s",LOG,storage_id);
    f = fopen(logfilename,"a");
  }

  if (f != NULL) {
    struct timeval tv;
    gettimeofday(&tv,NULL);

    // write microtimestamp
    fprintf(f, "%lu.%lu ",(unsigned long)tv.tv_sec,(unsigned long)tv.tv_usec);
    
    // write message
    va_list args;
    va_start( args, format );
    vfprintf( f, format, args );
    va_end( args );
    fclose(f);
  }
  pthread_mutex_unlock(&lock);
}


inline void debuglog(const char *format,...) {
#ifdef DEBUG
  FILE *f;
  char logfilename[MAX_PATH_LENGTH];
  pthread_mutex_lock(&lock);

  if (storage_id == NULL) {
    f = fopen(LOG,"a");
  } else {
    snprintf(logfilename, MAX_PATH_LENGTH,"%s%s",LOG,storage_id);
    f = fopen(logfilename,"a");
  }

  if (f != NULL) {
    struct timeval tv;
    gettimeofday(&tv,NULL);

    // write microtimestamp
    fprintf(f, "%lu.%lu DEBUG ",(unsigned long)tv.tv_sec,(unsigned long)tv.tv_usec);
    
    // write message
    va_list args;
    va_start( args, format );
    vfprintf( f, format, args );
    va_end( args );
    fclose(f);
  }
  pthread_mutex_unlock(&lock);
#endif
}


void write_changelog(const char *format,...) {
  /* 1. Check whether or not we should start a new file. */
  if((time(NULL)-CHANGELOGROTATE) > log_create_t) {
    if(log_fd != NULL) {
      flock(fileno(log_fd),LOCK_UN);
      fclose(log_fd);
    }
    log_fd = NULL;
  }

  /* 2. Make sure we have an open file to write to */
  if(log_fd == NULL) {
    log_create_t = time(NULL);
    asprintf(&rand_log_name,
	     "%s/%s-%ld-threadid=%08x",
	     CHANGELOGFOLDER,
	     storage_id,
	     log_create_t,
	     (unsigned int)pthread_self());
    log_fd = fopen(rand_log_name,"a");
    
    if(log_fd == NULL) {
      errorlog("Couldn't create file %s.",rand_log_name);
      return;
    }
      
    flock(fileno(log_fd),LOCK_EX);      
  }
  
  va_list args;
  va_start(args, format);
  vfprintf(log_fd, format, args);
  va_end(args);
  
  // We are writing to /dev/shm, so fflush is "cheap"
  fflush(log_fd);
}

inline void init_once(int dirfd) {
  /* On the first openat64 intercepted, lookup the path of dirfd on store
   * it.
   */
  if(dirpath == NULL) {
    pthread_mutex_lock(&lock);
    pid_t pid = getpid();
    char buf[256];
    sprintf(buf,"/proc/%u/fd/%u",pid,dirfd); // Assume this goes well
    char *dirpath_tmp = malloc(sizeof(char)*PATH_MAX);

    if (realpath(buf,dirpath_tmp) == NULL) {
      errorlog("failed resolving storage path (dirpath). path=%s\n", buf);
    }

    { // Retrieve a storage id for logfile seperation
      char *storage_id_tmp = strdup(dirpath_tmp);
      char * p = storage_id_tmp;
      while (*p != '\0') {
	if (*p == '/') {
	  *p = '-';
	}
	
	p++;
      }
      storage_id = storage_id_tmp;
    }
    pthread_mutex_unlock(&lock);
    dirpath = dirpath_tmp;

    if (strlen(dirpath) < 8) {
      errorlog("failed configuring dirpath. Must include '/chunks/'. dirpath=%s\n", dirpath);
    }
  }
    
}


int (*_original_openat)(int dirfd, const char *pathname, int flags, mode_t mode);
int openat64(int dirfd, const char *pathname, int flags, mode_t mode);

int openat64(int dirfd, const char *pathname, int flags, mode_t mode) {
  int fd = _original_openat(dirfd, pathname, flags, mode); 
  // If open() failed. Return without recording it.
  if(fd == -1) {
    debuglog("openat64() org-openat64() returned -1\n");
    return fd;
  }

  init_once(dirfd);

  debuglog("openat64() fd='%d', path='%s/%s'. flags='%d' mode='%lu'.\n",fd, dirpath,pathname,flags,(unsigned long)mode);

  // return on read only operation
  if (flags == 0) {
    return fd;
  }
  
  // read+write operation
  char *path = &pathbuf[MAX_PATH_LENGTH*(int)fd];
  strncpy(path, pathname,MAX_PATH_LENGTH);
  open_files[fd] = path;

  return fd;
}

int (*_original_unlinkat)(int dirfd, const char *pathname, int flags);
int unlinkat(int dirfd, const char *pathname, int flags);

int unlinkat(int dirfd, const char *pathname, int flags) {
  // unlinkat could be the first function called in this module. Use init_once()
  init_once(dirfd);
  int retval = _original_unlinkat(dirfd, pathname, flags); 
  if(retval == 0) {
    debuglog("unlinkat()      path='%s/%s'. Writing log.\n",dirpath,pathname);
    write_changelog("%llu d %s/%s\n",time(NULL),dirpath,pathname);
  } else {
    debuglog("unlinkat()      path='%s/%s'. unlinkat returned error, ignoring.\n",dirpath,pathname);
  }
  return retval;
}


int (*_original_close)(int fd);
int close(int fd);

int close(int fd) {
  if(open_files[fd] != NULL) {

    if(dirpath == NULL) {
      errorlog("close() dirpath == NULL on close. Shouldn't happen\n");
    }

    char filename[512];
    snprintf(filename,512,"%s/%s",dirpath,open_files[fd]);
    debuglog("close()    fd='%d', path='%s'. Writing log.\n",fd,filename);
    write_changelog("%llu m %s\n",time(NULL),filename);
    open_files[fd] = NULL;

  } else {
    writelog("close()    fd='%d', path='%s/%s'. No recorded openat for that fd, ignoring.\n",fd, dirpath,open_files[fd]);
  }

  int retval = _original_close(fd);

  return retval;
}

void init(void)__attribute__((constructor));

void init(void) {
  
  pthread_mutex_init(&lock,NULL);
  
  initlog();
  writelog("-----------\n");
  writelog("Initialising changelog folder...\n");
  
  int retval = mkdir(CHANGELOGFOLDER, S_IRUSR | S_IWUSR | S_IXUSR);
  // If log-dir creation fails for any other reason than folder-exists, exit.
  if(retval != 0 && errno != EEXIST) {
    writelog("Could not create changelog folder. '%s'. Not injecting library.\n", CHANGELOGFOLDER);
    errorlog("Could not create changelog folder. '%s'. Not injecting library.\n", CHANGELOGFOLDER);
    return;
  }
  else{
    writelog("Library injected.\n");
  }
  
  _original_openat = (int (*)(int, const char *, int, mode_t))
    dlsym(RTLD_NEXT, "openat64");
  _original_unlinkat = (int (*)(int, const char *, int))
    dlsym(RTLD_NEXT, "unlinkat");
  _original_close = (int (*)(int))
    dlsym(RTLD_NEXT, "close");
}

