include ../beegfs-conf.sh

CPPFLAGS?= -Wall -Wextra -pedantic -std=gnu99 -g -Os
LDFLAGS?=
SOURCES=perf.c filelist-runner.c getentry-runner.c
OBJECTS=$(SOURCES:.c=.o)
PROGRAMS=bp-cm-filelist bp-cm-getentry bp-set-corrupt

all: $(PROGRAMS)
	cp -f ../beegfs-conf.sh ../../bin/

clean:
	rm -f ${OBJECTS}
	cd ../../bin/ && rm -f ${PROGRAMS}

%.o: %.c Makefile
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $< -o $@

bp-cm-filelist: filelist-runner.o
	$(CC) $(LDFLAGS) -lpthread $^ -o $@
	mv -f $@ ../../bin/

bp-cm-getentry: perf.o getentry-runner.o
	$(CC) $(LDFLAGS) -lpthread $^ -o $@
	mv -f $@ ../../bin/

bp-set-corrupt: bp-set-corrupt.sh
	cp -f $^ $@
	mv -f $@ ../../bin/

