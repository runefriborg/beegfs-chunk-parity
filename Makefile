CPPFLAGS?=-Wall -Wextra -pedantic -std=gnu99 -g -O0
SOURCES=gen/main.c gen/file_info_hash.c rebuild/main.c common/task_processing.c common/persistent_db.c
OBJECTS=$(SOURCES:.c=.o)
PROGRAMS=bp-parity-gen bp-parity-rebuild

all: $(PROGRAMS)

clean:
	rm -f ${OBJECTS}
	rm -f ${PROGRAMS}

# Changing any header anywhere causes full recompile
%.o: %.c gen/*.h rebuild/*.h common/*.h Makefile
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $< -o $@

bp-parity-gen: gen/main.o gen/file_info_hash.o common/task_processing.o common/persistent_db.o
	$(CC) $(LDFLAGS) $^ -o $@
bp-parity-rebuild: rebuild/main.o common/task_processing.o common/persistent_db.o
	$(CC) $(LDFLAGS) $^ -o $@

