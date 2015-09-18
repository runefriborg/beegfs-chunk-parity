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


#define MAX_OPEN_FILES      16384
#define MAX_PATH_LENGTH     512
#define CHANGELOGROTATE     86400
#define CHANGELOGFOLDER     "/dev/shm/beegfs-changelog/"
#define LOG                 "/var/log/beegfs-changelog/intercept.log"
#define ERRORLOG            "/var/log/beegfs-changelog/error.log"
#define LOGDIR              "/var/log/beegfs-changelog"

#define DEBUG true

#define SAFE_TO_IGNORE ((char *)-1)

/* Operations for the `flock' call. */
#define    LOCK_SH    1    /* Shared lock.    */
#define    LOCK_EX    2    /* Exclusive lock. */
#define    LOCK_UN    8    /* Unlock.         */

/* Apply or remove an advisory lock, according to OPERATION,
   on the file FD refers to.  */
extern int flock (int __fd, int __operation);


char *open_files[MAX_OPEN_FILES];
char pathbuf[MAX_OPEN_FILES*MAX_PATH_LENGTH];
int is_initialized = 0;
char dirpath[PATH_MAX] = {0};
char storage_id[PATH_MAX] = {0};
__thread FILE *log_fd = NULL;
__thread time_t log_create_t = 0;
__thread char *log_name = NULL;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

/* function pointers to the original functions */
static int (*_original_openat)(int dirfd, const char *pathname, int flags, mode_t mode);
static int (*_original_unlinkat)(int dirfd, const char *pathname, int flags);
static int (*_original_close)(int fd);

static void write_log_msg(const char *basename, const char *msg) {
  FILE *f;
  char logfilename[MAX_PATH_LENGTH];
  pthread_mutex_lock(&lock);

  if (storage_id == NULL) {
    f = fopen(basename, "a");
  } else {
    snprintf(logfilename, MAX_PATH_LENGTH, "%s%s", basename, storage_id);
    f = fopen(logfilename, "a");
  }

  if (f != NULL) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    fprintf(f, "%lu.%lu %s", (unsigned long)tv.tv_sec, (unsigned long)tv.tv_usec, msg);
    fflush(f);
    fclose(f);
  }
  pthread_mutex_unlock(&lock);
}

static void errorlog(const char *format,...) {
  char msg[200] = {0};
  va_list args;
  va_start(args, format);
  vsnprintf(msg, sizeof(msg), format, args);
  va_end(args);
  write_log_msg(ERRORLOG, msg);
}

static void writelog(const char *format,...) {
  char msg[200] = {0};
  va_list args;
  va_start(args, format);
  vsnprintf(msg, sizeof(msg), format, args);
  va_end(args);
  write_log_msg(LOG, msg);
}


static void debuglog(const char *format,...) {
#ifdef DEBUG
  char msg_a[200] = {0};
  char msg[200] = {0};
  va_list args;
  va_start(args, format);
  vsnprintf(msg_a, sizeof(msg_a), format, args);
  va_end(args);
  snprintf(msg, sizeof(msg), "DEBUG %s", msg_a);
  write_log_msg(LOG, msg);
#endif
}

static
void write_changelog(const char *format,...) {
  /* 1. Check whether or not we should start a new file. */
  time_t now = time(NULL);
  if((now-CHANGELOGROTATE) > log_create_t) {
    if(log_fd != NULL) {
      flock(fileno(log_fd),LOCK_UN);
      fclose(log_fd);
    }
    log_fd = NULL;
  }

  /* 2. Make sure we have an open file to write to */
  if(log_fd == NULL) {
    log_create_t = now;
    asprintf(&log_name,
        "%s/%s-%ld-%08x",
        CHANGELOGFOLDER,
        storage_id,
        log_create_t,
        (unsigned int)pthread_self());
    log_fd = fopen(log_name,"a");

    if(log_fd == NULL) {
      errorlog("Couldn't create file %s.\n",log_name);
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

static void init_once(int dirfd) {
  /* On the first openat64 intercepted, store the path of dirfd */
  if(is_initialized == 0) {
    pthread_mutex_lock(&lock);
    if (is_initialized != 0) {
      pthread_mutex_unlock(&lock);
      return;
    }
    pid_t pid = getpid();
    char buf[256];
    sprintf(buf,"/proc/%u/fd/%u",pid,dirfd); // Assume this goes well

    if (realpath(buf,dirpath) == NULL) {
      errorlog("failed resolving storage path (dirpath). path=%s\n", buf);
    }

    // Retrieve a storage id for logfile seperation
    strcpy(storage_id, dirpath);
    char *p = storage_id;
    while (*p != '\0') {
      if (*p == '/') {
        *p = '-';
      }
      p++;
    }
    is_initialized = 1;
    pthread_mutex_unlock(&lock);

    if (strlen(dirpath) < 8) {
      errorlog("failed configuring dirpath. Must include '/chunks/'. dirpath=%s\n", dirpath);
    }

    writelog("init_once() initialized for %s\n", storage_id);
  }
}

int openat64(int dirfd, const char *pathname, int flags, mode_t mode) {
  int fd = _original_openat(dirfd, pathname, flags, mode);

  init_once(dirfd);

  debuglog("openat64() fd='%d', path='%s/%s'. flags='%d' mode='%lu'.\n",fd, dirpath,pathname,flags,(unsigned long)mode);

  // If open() failed. Return without recording it.
  if(fd == -1) {
    debuglog("openat64() org-openat64() returned -1\n");
    return fd;
  }

  // return on read only operation
  if (flags == 0) {
    open_files[fd] = SAFE_TO_IGNORE;
    return fd;
  }

  // read+write operation
  char *path = &pathbuf[MAX_PATH_LENGTH*(int)fd];
  strncpy(path, pathname,MAX_PATH_LENGTH);
  open_files[fd] = path;

  return fd;
}

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

int close(int fd) {
  if (fd < 0 || fd > MAX_OPEN_FILES) {
    errorlog("close() fd is invalid - this should never happen");
    return _original_close(fd);
  }

  const char *fd_info = open_files[fd];
  if (fd_info == SAFE_TO_IGNORE) {
    /* Do nothing. */
  }
  else if (fd_info == NULL) {
    pid_t pid = getpid();
    char buf[256];
    char buf2[PATH_MAX];
    sprintf(buf,"/proc/%u/fd/%u",pid,fd);
    realpath(buf,buf2);
    writelog("close()    fd='%d'. No recorded open/openat for this fd - realpath: %s\n",fd,buf2);
  }
  else {
    if (!is_initialized) {
      errorlog("close() not initialized on close. Shouldn't happen\n");
    }

    char filename[512];
    snprintf(filename,sizeof(filename),"%s/%s",dirpath,open_files[fd]);
    debuglog("close()    fd='%d', path='%s'. Writing log.\n",fd,filename);
    write_changelog("%llu m %s\n",time(NULL),filename);
    open_files[fd] = NULL;
  }

  return _original_close(fd);
}

static void __attribute__((constructor)) init(void) {
  /* Make sure the log folder exists */
  int ret = mkdir(LOGDIR, S_IRUSR | S_IWUSR | S_IXUSR);
  if(ret != 0 && errno != EEXIST) {
    fprintf(stderr, "Could not create logdir '%s'.\n", LOGDIR);
    exit(1);
  }

  writelog("-----------\n");
  writelog("Library injected.\n");

  int retval = mkdir(CHANGELOGFOLDER, S_IRUSR | S_IWUSR | S_IXUSR);
  // If log-dir creation fails for any other reason than folder-exists, exit.
  if (retval != 0 && errno != EEXIST) {
    errorlog("Could not create changelog folder. '%s'. Not injecting library.\n", CHANGELOGFOLDER);
    exit(1);
  }

  _original_openat = (int (*)(int, const char *, int, mode_t))
    dlsym(RTLD_NEXT, "openat64");
  _original_unlinkat = (int (*)(int, const char *, int))
    dlsym(RTLD_NEXT, "unlinkat");
  _original_close = (int (*)(int))
    dlsym(RTLD_NEXT, "close");

  if (_original_openat == NULL
          || _original_unlinkat == NULL
          || _original_close == NULL) {
      errorlog("Cannot load original functions, we are really screwed!\n");
      exit(1);
  }
}
