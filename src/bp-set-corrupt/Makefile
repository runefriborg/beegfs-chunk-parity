include ../beegfs-conf.sh

CPPFLAGS?= -Wall -Wextra -pedantic -std=gnu99 -g -Os
LDFLAGS?=
SOURCES=perf.c filelist-runner.c getentry-runner.c
OBJECTS=$(SOURCES:.c=.o)
PROGRAMS=bp-cm-filelist bp-cm-getentry bp-set-corrupt

all: $(PROGRAMS)

clean:
	rm -f ${OBJECTS}
	rm -f ${PROGRAMS}

%.o: %.c Makefile
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $< -o $@

bp-cm-filelist: filelist-runner.o
	$(CC) $(LDFLAGS) -lpthread $^ -o $@

bp-cm-getentry: perf.o getentry-runner.o
	$(CC) $(LDFLAGS) -lpthread $^ -o $@

bp-set-corrupt: bp-set-corrupt.sh
	cp -f $^ $@

