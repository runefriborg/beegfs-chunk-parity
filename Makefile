CPPFLAGS?=-Wall -Wextra -pedantic -std=gnu99 -g -O0
SOURCES=xx.c
OBJECTS=$(SOURCES:.c=.o)
PROGRAMS=xx

all: $(PROGRAMS)

clean:
	rm -f ${OBJECTS}
	rm -f ${PROGRAMS}

%.o: %.c Makefile
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $< -o $@

xx: xx.o
	$(CC) $(LDFLAGS) $^ -o $@

