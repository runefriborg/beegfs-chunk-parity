#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>

int count_files(char *path)
{
    int sum = 0;
    DIR *dir;

    dir = opendir(path);
    if (!dir) {
        perror("opendir");
        exit(EXIT_FAILURE);
    }

    for (;;) {
        struct dirent entry;
        struct dirent *result;
        char *name;
        char sub_path[256];

        int error = readdir_r(dir, &entry, &result);
        if (error != 0) {
            perror("readdir");
            exit(EXIT_FAILURE);
        }

        // readdir_r returns NULL in *result if the end 
        // of the directory stream is reached
        if (result == NULL)
            break;

        name = result->d_name;
        if ((strcmp(name, ".") == 0) || (strcmp(name, "..") == 0))
            continue;

        sum++;
        if (result->d_type == DT_DIR) {
            sprintf(sub_path, "%s/%s", path, name); 
            sum += count_files(sub_path);
        }
    }
    closedir(dir);

    return sum;
}

int main()
{
    char *path = "/faststorage/project/DanishPanGenome2";
    int sum = count_files(path);
    printf("There are %d files in '%s'\n", sum, path);
    return 0;
}
