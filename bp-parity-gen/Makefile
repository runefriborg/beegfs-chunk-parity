CPPFLAGS?=-Wall -Wextra -pedantic -std=gnu99 -g -O0
SOURCES=main.c file_info_hash.c task_processing.c persistent_db.c
OBJECTS=$(SOURCES:.c=.o)
PROGRAMS=bp-parity-gen

all: $(PROGRAMS)

clean:
	rm -f ${OBJECTS}
	rm -f ${PROGRAMS}

%.o: %.c *.h Makefile
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $< -o $@

bp-parity-gen: main.o file_info_hash.o task_processing.o persistent_db.o
	$(CC) $(LDFLAGS) $^ -o $@

