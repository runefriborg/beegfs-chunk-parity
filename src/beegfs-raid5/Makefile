include ../beegfs-conf.sh

CC=mpicc
CPPFLAGS?=-Wall -Wextra -pedantic -std=gnu99 -I$(CONF_LEVELDB_INCLUDEPATH) -g -O0
SOURCES=gen/main.c gen/file_info_hash.c gen/assign_lanes.c rebuild/main.c common/progress_reporting.c common/task_processing.c common/persistent_db.c
OBJECTS=$(SOURCES:.c=.o)
PROGRAMS=../../bin/bp-parity-gen ../../bin/bp-parity-rebuild

all: $(PROGRAMS)

clean:
	rm -f ${OBJECTS}
	rm -f ${PROGRAMS}

# Changing any header anywhere causes full recompile
%.o: %.c gen/*.h rebuild/*.h common/*.h Makefile
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $< -o $@

../../bin/bp-parity-gen: gen/main.o gen/file_info_hash.o gen/assign_lanes.o common/progress_reporting.o common/task_processing.o common/persistent_db.o
	$(CC) -L$(CONF_LEVELDB_LIBPATH) -lleveldb -lpthread $(LDFLAGS) $^ -o $@
../../bin/bp-parity-rebuild: rebuild/main.o common/progress_reporting.o common/task_processing.o common/persistent_db.o
	$(CC) -L$(CONF_LEVELDB_LIBPATH) -lleveldb $(LDFLAGS) $^ -o $@

