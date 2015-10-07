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
#include <err.h>
#include <syslog.h>

/* MAX_OPEN_FILES should match the limit set for the beegfs-storage process. */
#define MAX_OPEN_FILES              60000
#define MAX_PATH_LENGTH             512
#define CHANGELOG_ROTATION_TIME     86400
#define CHANGELOG_FOLDER            "/dev/shm/beegfs-changelog/"

/* DEBUG must be defined, change to 0 to disable debug info */
#define DEBUG 1

#define SAFE_TO_IGNORE ((char *)-1)

/* Operations for the `flock' call. */
#define    LOCK_SH    1    /* Shared lock.    */
#define    LOCK_EX    2    /* Exclusive lock. */
#define    LOCK_UN    8    /* Unlock.         */

/* Apply or remove an advisory lock, according to OPERATION,
   on the file FD refers to.  */
extern int flock (int __fd, int __operation);

/* Initialized once, when library is loaded */
static char storage_id[PATH_MAX] = {0};
static char dirpath[PATH_MAX] = {0};
static int (*_original_openat)(int dirfd, const char *pathname, int flags, mode_t mode);
static int (*_original_unlinkat)(int dirfd, const char *pathname, int flags);
static int (*_original_close)(int fd);

/* Initialized per thread as needed */
static __thread FILE *changelog_fd = NULL;
static __thread time_t changelog_create_time = 0;
static __thread char *changelog_name = NULL;

/* Shared between threads. No protection because we assume the BeeGFS storage
 * daemon is properly multi-threaded and only uses each file-descriptor from
 * one thread at a time. */
static char *open_files[MAX_OPEN_FILES];
static char pathbuf[MAX_OPEN_FILES*MAX_PATH_LENGTH];

/* The ', ##__VA_ARGS__' makes the param list optional */
#define log_error(fmt, ...) syslog(LOG_ERR, fmt, ##__VA_ARGS__)
#define log_info(fmt, ...) syslog(LOG_INFO, fmt, ##__VA_ARGS__)

#if DEBUG
#define log_debug(fmt, ...) syslog(LOG_INFO, "DEBUG " fmt, ##__VA_ARGS__)
#else
#define log_debug(...)
#endif

static
void write_change(const char *format,...) {
  /* 1. Check whether or not we should start a new file. */
  time_t now = time(NULL);
  if (difftime(now, changelog_create_time) > CHANGELOG_ROTATION_TIME) {
    if(changelog_fd != NULL) {
      flock(fileno(changelog_fd),LOCK_UN);
      fclose(changelog_fd);
    }
    changelog_fd = NULL;
  }

  /* 2. Make sure we have an open file to write to */
  if(changelog_fd == NULL) {
    changelog_create_time = now;
    asprintf(&changelog_name,
        "%s/%s-%ld-%08x",
        CHANGELOG_FOLDER,
        storage_id,
        changelog_create_time,
        (unsigned int)pthread_self());
    changelog_fd = fopen(changelog_name,"a");

    if(changelog_fd == NULL) {
      log_error("Cant create changelog %s", changelog_name);
      return;
    }

    flock(fileno(changelog_fd),LOCK_EX);
  }

  va_list args;
  va_start(args, format);
  vfprintf(changelog_fd, format, args);
  va_end(args);

  // We are writing to /dev/shm, so fflush is "cheap"
  fflush(changelog_fd);
}

int openat64(int dirfd, const char *pathname, int flags, mode_t mode) {
  int fd = _original_openat(dirfd, pathname, flags, mode);
  int _errno = errno;

  if (fd < -1 || fd >= MAX_OPEN_FILES) {
    log_error("openat64() fd is invalid. fd='%d'", fd);
    errno = _errno;
    return fd;
  }

  // If open() failed. Return without recording it.
  if(fd == -1) {
    log_debug("openat64() returned -1 (error: %s)", strerror(_errno));
    errno = _errno;
    return fd;
  }

  char *old_path = open_files[fd];
  if (old_path != NULL) {
      log_error("openat64() fd %d was not cleared, old path = '%s'",
              fd,
              old_path == SAFE_TO_IGNORE? "(SAFE_TO_IGNORE)" : old_path);
  }

  log_debug("openat64() path='%s/%s'. flags='%d' mode='%lu'",
      dirpath, pathname, flags, (unsigned long)mode);

  // return on read only operation
  if (flags == 0)
    open_files[fd] = SAFE_TO_IGNORE;
  else {
    // read+write operation
    char *path = &pathbuf[MAX_PATH_LENGTH*(int)fd];
    strncpy(path, pathname,MAX_PATH_LENGTH);
    open_files[fd] = path;
  }
  errno = _errno;
  return fd;
}

int unlinkat(int dirfd, const char *pathname, int flags) {
  int retval = _original_unlinkat(dirfd, pathname, flags);
  int _errno = errno;
  if(retval == 0) {
    log_debug("unlinkat()      path='%s/%s'", dirpath, pathname);
    write_change("%llu d %s/%s\n", time(NULL), dirpath, pathname);
  } else {
    log_debug("unlinkat()      path='%s/%s'. error: %s",
        dirpath, pathname, strerror(_errno));
  }
  errno = _errno;
  return retval;
}

int close(int fd) {
  if (fd < 0 || fd >= MAX_OPEN_FILES) {
    log_error("close() fd is invalid. fd='%d'", fd);
    return _original_close(fd);
  }

  const char *fd_info = open_files[fd];
  if (fd_info == SAFE_TO_IGNORE) {
    /* Do nothing. */
  }
  else if (fd_info == NULL) {
#if DEBUG
    char buf[256];
    char buf2[PATH_MAX];
    sprintf(buf,"/proc/self/fd/%u",fd);
    realpath(buf,buf2);
    log_debug("close()    fd='%d'. No recorded openat on %s", fd, buf2);
#endif
  }
  else {
    log_debug("close()    fd='%d', path='%s/%s'", fd, dirpath, fd_info);
    write_change("%llu m %s/%s\n",time(NULL),dirpath,fd_info);
  }

  open_files[fd] = NULL;

  return _original_close(fd);
}

static void __attribute__((constructor)) init(void) {
  const char *store = getenv("BP_STORE");
  if (store == NULL) {
    errx(1, "Needs BP_STORE to be set");
  }
  snprintf(dirpath, PATH_MAX, "/%s/chunks", store);
  strncpy(storage_id, store, PATH_MAX-1);

  int retval = mkdir(CHANGELOG_FOLDER, S_IRUSR | S_IWUSR | S_IXUSR);
  // If log-dir creation fails for any other reason than folder-exists, exit.
  if (retval != 0 && errno != EEXIST) {
    err(1, "Could not create changelog folder '%s'", CHANGELOG_FOLDER);
  }

  log_info("BeeGFS changelogger library injected");

  _original_openat = dlsym(RTLD_NEXT, "openat64");
  _original_unlinkat = dlsym(RTLD_NEXT, "unlinkat");
  _original_close = dlsym(RTLD_NEXT, "close");

  if (_original_openat == NULL
          || _original_unlinkat == NULL
          || _original_close == NULL) {
      errx(1, "Cannot load original functions, we are really screwed!\n");
  }
}
