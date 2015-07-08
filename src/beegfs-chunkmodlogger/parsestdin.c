#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>

static void parse_inputstream(FILE *input_file)
{
    char buf[64*1024];
    ssize_t buf_size = sizeof(buf);
    ssize_t buf_offset = 0;
    int read;
    while ((read = fread(buf + buf_offset, 1, buf_size - buf_offset, input_file)) > 0)
    {
        size_t buf_alive = buf_offset + read;
        const char *bufp = buf;
        while (buf_alive >= 3*sizeof(uint64_t)) {
            int64_t timestamp_secs = ((int64_t *)bufp)[0];
            char        event_type = (char)((uint64_t *)bufp)[1];
            uint64_t   len_of_path = ((uint64_t *)bufp)[2];
            if (3*sizeof(uint64_t) + len_of_path > buf_alive) {
                buf_offset = buf_alive;
                memmove(buf, bufp, buf_alive);
                break;
            }
            const char *path = bufp + 3*sizeof(uint64_t);
            bufp += len_of_path + 1 + 3*sizeof(uint64_t);
            buf_alive -= len_of_path + 1 + 3*sizeof(uint64_t);
	    printf("%i %c %u %s\n", timestamp_secs, event_type, len_of_path, path);
        }
        if (3*sizeof(uint64_t) >= buf_alive) {
            buf_offset = buf_alive;
            memmove(buf, bufp, buf_alive);
        }
    }
}

/* Use the same method for parse as in beegfs-raid5. */
int main(int argc, char **argv)
{
    parse_inputstream(stdin);
    printf("\n");
}
