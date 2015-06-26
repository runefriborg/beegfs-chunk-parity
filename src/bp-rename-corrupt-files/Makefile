include ../beegfs-conf.sh

CPPFLAGS?=-Wall -Wextra -pedantic -std=gnu99 -I$(CONF_LEVELDB_INCLUDEPATH) -g -Os
SOURCES=main.c
OBJECTS=$(SOURCES:.c=.o)
PROGRAMS=../../bin/bp-rename-corrupt-files

all: $(PROGRAMS)

clean:
	rm -f ${OBJECTS}
	rm -f ${PROGRAMS}

%.o: %.c *.h Makefile
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $< -o $@

../../bin/bp-rename-corrupt-files: main.o
	$(CC) -L$(CONF_LEVELDB_LIBPATH) -lleveldb $(LDFLAGS) $^ -o $@

