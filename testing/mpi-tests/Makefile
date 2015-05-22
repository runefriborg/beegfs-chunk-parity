CPPFLAGS?=-Wall -Wextra -pedantic -std=gnu99 -g -Os
SOURCES=mpi-tasklist-test.c mpi-bandwidth.c mpi-async-bandwidth.c
OBJECTS=$(SOURCES:.c=.o)
PROGRAMS=mpi-tasklist-test mpi-bandwidth mpi-async-bandwidth

all: $(PROGRAMS)

clean:
	rm -f ${OBJECTS}
	rm -f ${PROGRAMS}

%.o: %.c Makefile
	$(CC) -c $(CPPFLAGS) $(CFLAGS) $< -o $@

mpi-tasklist-test: mpi-tasklist-test.o
	$(CC) $(LDFLAGS) $^ -o $@

mpi-bandwidth: mpi-bandwidth.o
	$(CC) $(LDFLAGS) -lrt $^ -o $@
mpi-async-bandwidth: mpi-async-bandwidth.o
	$(CC) $(LDFLAGS) -lrt $^ -o $@

