CPPFLAGS?=-Wall -Wextra -pedantic -std=gnu99 -g -O0
SOURCES=gen/main.c gen/file_info_hash.c common/task_processing.c common/persistent_db.c
OBJECTS=$(SOURCES:.c=.o)
PROGRAMS=bp-parity-gen

all: $(PROGRAMS)

clean:
	rm -f ${OBJECTS}
	rm -f ${PROGRAMS}

%.o: %.c gen/*.h common/*.h Makefile
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $< -o $@

bp-parity-gen: gen/main.o gen/file_info_hash.o common/task_processing.o common/persistent_db.o
	$(CC) $(LDFLAGS) $^ -o $@

