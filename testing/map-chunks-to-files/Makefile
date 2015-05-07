CPPFLAGS?=-Wall -Wextra -pedantic -std=gnu99 -g -Os
SOURCES=filelist-runner.c
OBJECTS=$(SOURCES:.c=.o)
PROGRAMS=filelist-runner

all: $(PROGRAMS)

clean:
	rm -f ${OBJECTS}
	rm -f ${PROGRAMS}

%.o: %.c Makefile
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $< -o $@

filelist-runner: filelist-runner.o
	$(CC) $(LDFLAGS) -lpthread $^ -o $@
