CPPFLAGS?=-Wall -Wextra -pedantic -std=gnu99 -g -O0
SOURCES=xx.c file_info_hash.c task_processing.c
OBJECTS=$(SOURCES:.c=.o)
PROGRAMS=xx

all: $(PROGRAMS)

clean:
	rm -f ${OBJECTS}
	rm -f ${PROGRAMS}

%.o: %.c *.h Makefile
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $< -o $@

xx: xx.o file_info_hash.o task_processing.o
	$(CC) $(LDFLAGS) $^ -o $@

