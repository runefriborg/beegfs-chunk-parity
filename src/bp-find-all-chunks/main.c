#include <string.h>

#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>

#include <ftw.h>

#define MODIFY_EVENT 'm'
#define UNLINK_EVENT 'd'

char buffer[64*1024] = {0};
int buffer_written = 0;

int visitor(const char *fpath, const struct stat *sb, int typeflag)
{
    if (typeflag != FTW_F)
        return 0;
    size_t len = strlen(fpath);
    /* Skip './' in each path */
    fpath += 2;
    len -= 2;
    size_t fields[4] = {sb->st_mtime, sb->st_size, MODIFY_EVENT, len};
    if (sizeof(fields) + len + buffer_written >= sizeof(buffer)) {
        write(1, buffer, buffer_written);
        buffer_written = 0;
    }
    memcpy(buffer + buffer_written, &fields, sizeof(fields));
    buffer_written += sizeof(fields);
    memcpy(buffer + buffer_written, fpath, len);
    buffer_written += len;
    return 0;
}

int main(int argc, char **argv)
{
    if (argc != 2) {
        return 1;
    }
    chdir(argv[1]);
    ftw(".", visitor, 100);
    write(1, buffer, buffer_written);
    return 0;
}
