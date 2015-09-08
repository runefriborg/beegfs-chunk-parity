include ../beegfs-conf.sh

CPPFLAGS?=-I${CONF_LEVELDB_INCLUDEPATH} -Wall -Wextra -pedantic -std=gnu99 -g -Os
LDFLAGS?=-L${CONF_LEVELDB_LIBPATH}
SOURCES=perf.c filelist-runner.c getentry-runner.c
OBJECTS=$(SOURCES:.c=.o)
PROGRAMS=bp-cm-filelist bp-cm-getentry bp-chunkmap

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
	$(CC) $(LDFLAGS) -lleveldb -lpthread $^ -o $@
	mv -f $@ ../../bin/

bp-chunkmap: beegfs-chunkmap.sh
	cp -f $^ $@
	mv -f $@ ../../bin/

