#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

int list(const char *name, const struct stat *status, int type);

int main(int argc, char *argv[]) {
 if(argc == 1)
  ftw(".", list, 1);
 else
  ftw(argv[1], list, 1);

 return 0;
}

struct chunk_id_buf_t {
	char output[1035];
	char cmd[512];
};

int get_chunk_id(const char *name) {
  FILE *fp;
  struct chunk_id_buf_t buf;
  struct chunk_id_buf_t * p = &buf;

  snprintf(p->cmd, sizeof(p->cmd), "/usr/bin/fhgfs-ctl --getentryinfo %s", name);

  /* Open the command for reading. */
  fp = popen(p->cmd, "r");
  if (fp == NULL) {
    printf("Failed to run command\n" );
    exit(1);
  }

  /* Read the output a line at a time - output it. */
  while (fgets(p->output, sizeof(p->output)-1, fp) != NULL) {
    printf("%s", p->output);
  }

  /* close */
  pclose(fp);

  return 0;
}


int list(const char *name, const struct stat *status, int type) {
 if(type == FTW_NS)
  return 0;

 if(type == FTW_F)
  printf("0%3o\t%s\n", status->st_mode&0777, name);
  get_chunk_id(name);
 
 if(type == FTW_D && strcmp(".", name) != 0)
  printf("0%3o\t%s/\n", status->st_mode&0777, name);

 return 0;
}
