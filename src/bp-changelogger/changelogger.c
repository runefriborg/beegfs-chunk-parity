#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <errno.h>
#include <dlfcn.h>
#include <time.h>
#include <stdarg.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdbool.h>
#include <limits.h>


//Stole these from asm-generic/fcntl.h
//#define O_WRONLY        00000001
//#define O_RDWR          00000002

#define MAX_OPEN_FILES 65535
#define MAX_PATH_LENGTH 512
#define LOGROTATE 86400
#define LOGFOLDER      "/dev/shm/chunkmod_intercept/"
#define LOGFILE_PREFIX "close_logging"
#define DEBUGLOG       "/var/log/chunkmod-logs/chunkmod-intercept.log"
#define DEBUGDIR       "/var/log/chunkmod-logs"


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
char *dirpath;
__thread FILE *log_fd = NULL;
__thread time_t log_create_t = 0;
__thread char *rand_log_name = NULL;
#ifdef DEBUG
pthread_mutex_t lock;
#endif

/* Check a bitmask for set bits. */
int is_set(int bitmask,int to_check_for)
{
	return to_check_for == (bitmask & to_check_for);
}

inline void debug(const char *format,...)
{
#ifdef DEBUG
	pthread_mutex_lock(&lock);
	char debugfilename[512];
	snprintf(debugfilename,512,"%s.%08x",DEBUGLOG,(unsigned int)pthread_self());
	FILE *f = fopen(debugfilename,"a");
	if (f != NULL){
		time_t ltime;
		ltime = time(NULL);
		char *timestamp = asctime( localtime(&ltime));
		timestamp[strlen(timestamp)-1] = ' ';

		struct timeval tv;
		gettimeofday(&tv,NULL);
		char microtimestamp[256];
		snprintf(microtimestamp,256,"%lu.%lu ",(unsigned long)tv.tv_sec,(unsigned long)tv.tv_usec);

		fprintf(f, "%s", microtimestamp);
		va_list args;
		va_start( args, format );
		vfprintf( f, format, args );
		va_end( args );
		fclose(f);
	}
	pthread_mutex_unlock(&lock);
#endif
}
void write_log(const char *format,...)
{
	/* 1. Check whether or not we should start a new file. */
	if((time(NULL)-LOGROTATE) > log_create_t)
	{
		if(log_fd != NULL){
                        flock(fileno(log_fd),LOCK_UN);
			fclose(log_fd);
                }
		log_fd = NULL;
	}
	/* 2. Make sure we have an open file to write to */
	if(log_fd == NULL)
	{
		log_create_t = time(NULL);
		asprintf(&rand_log_name,
			 "%s/%s-%ld-threadid=%08x",
			 LOGFOLDER,
			 LOGFILE_PREFIX,
			 log_create_t,
			 (unsigned int)pthread_self());
		log_fd = fopen(rand_log_name,"a");

		// TODO: make beegfs crash with file-open-error?
		if(log_fd == NULL){
			debug("Couldn't create file %s.",rand_log_name);
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

inline void init_once(int dirfd)
{
	/* On the first openat64 intercepted, lookup the path of dirfd on store
	 * it.
	 */
	if(dirpath == NULL){
		pthread_mutex_lock(&lock);
		pid_t pid = getpid();
		char buf[256];
		sprintf(buf,"/proc/%u/fd/%u",pid,dirfd); // Assume this goes well
		char *dirpath_tmp = malloc(sizeof(char)*PATH_MAX);
		realpath(buf,dirpath_tmp);
		pthread_mutex_unlock(&lock);
		dirpath = dirpath_tmp;
	}
}


int (*_original_openat)(int dirfd, const char *pathname, int flags, mode_t mode);
int openat64(int dirfd, const char *pathname, int flags, mode_t mode);

int openat64(int dirfd, const char *pathname, int flags, mode_t mode)
{
	int fd = _original_openat(dirfd, pathname, flags, mode); 
	// If open() failed. Return without recording it.
	if(fd == -1){
		debug("openat64() org-openat64() returned -1\n");
		return fd;
	}

	init_once(dirfd);

	debug("openat64() fd='%d', path='%s/%s'. threadid='%08x' flags='%d' mode='%lu'.\n",fd, dirpath,pathname,(unsigned int)pthread_self(),flags,(unsigned long)mode);

	if (flags == 0)
		return fd;

	char *path = &pathbuf[MAX_PATH_LENGTH*(int)fd];
	path = strncpy(path, pathname,MAX_PATH_LENGTH);

	// Insert path no matter what while debugging.
	open_files[fd] = path;

	return fd;
}

int (*_original_unlinkat)(int dirfd, const char *pathname, int flags);
int unlinkat(int dirfd, const char *pathname, int flags);

int unlinkat(int dirfd, const char *pathname, int flags)
{
        // unlinkat could be the first function called in this module. Use init_once()
        init_once(dirfd);
	int retval = _original_unlinkat(dirfd, pathname, flags); 
	if(retval == 0) {
		debug("unlinkat()      path='%s/%s'. Writing log.\n",dirpath,pathname);
		write_log("%llu d %s/%s\n",time(NULL),dirpath,pathname);
	}
	else
		debug("unlinkat()      path='%s/%s'. unlinkat returned error, ignoring.\n",dirpath,pathname);

	return retval;
}


int (*_original_close)(int fd);
int close(int fd);

int close(int fd)
{
	if(dirpath == NULL)
	{
		debug("close() dirpath == NULL on close. Shouldn't happen\n");
	}
	if(open_files[fd] != NULL)
	{
		char filename[512];
		snprintf(filename,512,"%s/%s",dirpath,open_files[fd]);
		debug("close()    fd='%d', path='%s'. threadid='%08x'.  Writing log.\n",fd,filename,(unsigned int)pthread_self());
		//char *path = open_files[fd];
		write_log("%llu m %s\n",time(NULL),filename);
		open_files[fd] = NULL;
	}
	else{
		debug("close()    fd='%d', path='%s/%s'. No recorded openat for that fd, ignoring.\n",fd, dirpath,open_files[fd]);
	}

	int retval = _original_close(fd);

	return retval;
}

void init(void)__attribute__((constructor));

void init(void)
{
	pthread_mutex_init(&lock,NULL);

#ifdef DEBUG
	int ret = mkdir(DEBUGDIR, S_IRUSR | S_IWUSR | S_IXUSR);
	if(ret != 0 && errno != EEXIST) {
		printf("Could not create debug-dir '%s'.\n", DEBUGDIR);
		return;
	}
#endif

	debug("-----------\n");
	debug("Creating log-dir...\n");

	int retval = mkdir(LOGFOLDER, S_IRUSR | S_IWUSR | S_IXUSR);
	// If log-dir creation fails for any other reason than folder-exists, exit.
	if(retval != 0 && errno != EEXIST) {
		debug("Could not create log-dir. '%s'. Not injecting library.\n", LOGFOLDER);
		return;
	}
	else{
		debug("Library injected.\n");
	}

	_original_openat = (int (*)(int, const char *, int, mode_t))
		dlsym(RTLD_NEXT, "openat64");
	_original_unlinkat = (int (*)(int, const char *, int))
		dlsym(RTLD_NEXT, "unlinkat");
	_original_close = (int (*)(int))
		dlsym(RTLD_NEXT, "close");
}

