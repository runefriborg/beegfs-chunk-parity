#include <string.h>

#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>

#include <ftw.h>

char buffer[64*1024] = {0};
int buffer_written = 0;

int visitor(const char *fpath, const struct stat *sb, int typeflag)
{
    if (typeflag != FTW_F)
        return 0;
    size_t len = strlen(fpath);
    size_t fields[3] = {sb->st_mtime,(size_t)'m',len};
    if (sizeof(fields) + len + buffer_written >= sizeof(buffer)) {
        write(1, buffer, buffer_written);
        buffer_written = 0;
    }
    memcpy(buffer + buffer_written, &fields, sizeof(fields));
    buffer_written += sizeof(fields);
    memcpy(buffer + buffer_written, fpath, len);
    buffer[buffer_written + len] = 0;
    buffer_written += len + 1;
    return 0;
}

int main(int argc, char **argv)
{
    if (argc != 2) {
        return 1;
    }
    ftw(argv[1], visitor, 100);
    write(1, buffer, buffer_written);
    return 0;
}
