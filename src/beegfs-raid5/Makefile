include ../beegfs-conf.sh

GIT_COMMIT=0x$(shell git rev-parse --short=8 HEAD)
CC=mpicc
CPPFLAGS?=-Wall -Wextra -pedantic -std=gnu99 -I$(CONF_LEVELDB_INCLUDEPATH) -g -O0
CPPFLAGS+=-D_GIT_COMMIT=${GIT_COMMIT}
CPPFLAGS+=-DMAX_WORKITEMS=${MAX_ITEMS}
SOURCES=gen/main.c gen/file_info_hash.c gen/assign_lanes.c rebuild/main.c common/progress_reporting.c common/task_processing.c common/persistent_db.c
OBJECTS=$(SOURCES:.c=.o)
PROGRAMS=bp-parity-gen bp-parity-rebuild

all: $(PROGRAMS)

clean:
	rm -f ${OBJECTS}
	rm -f ${PROGRAMS}

bp-parity-gen: gen/main.o gen/file_info_hash.o gen/assign_lanes.o common/progress_reporting.o common/task_processing.o common/persistent_db.o
	$(CC) -L$(CONF_LEVELDB_LIBPATH) -lleveldb -lpthread -lm $(LDFLAGS) $^ -o $@
bp-parity-rebuild: rebuild/main.o common/progress_reporting.o common/task_processing.o common/persistent_db.o
	$(CC) -L$(CONF_LEVELDB_LIBPATH) -lleveldb $(LDFLAGS) $^ -o $@

