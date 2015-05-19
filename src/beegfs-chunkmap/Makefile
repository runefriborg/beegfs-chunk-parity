LEVELDB=/project/SystemWork/leveldb

CPPFLAGS?=-I${LEVELDB}/include -Wall -Wextra -pedantic -std=gnu99 -g -Os
LDFLAGS?=-L${LEVELDB}
SOURCES=perf.c filelist-runner.c getentry-runner.c
OBJECTS=$(SOURCES:.c=.o)
PROGRAMS=filelist-runner getentry-runner

all: $(PROGRAMS)

clean:
	rm -f ${OBJECTS}
	rm -f ${PROGRAMS}

%.o: %.c Makefile
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $< -o $@

filelist-runner: filelist-runner.o
	$(CC) $(LDFLAGS) -lpthread $^ -o $@

getentry-runner: perf.o getentry-runner.o
	$(CC) $(LDFLAGS) -lleveldb -lpthread $^ -o $@
