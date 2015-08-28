CC=gcc
CFLAGS=-I.

chunkmod_intercept: chunkmod_intercept.c
	$(CC) -fPIC -Wall -shared -o chunkmod_intercept.{so,c}  -ldl

parsestdin: parsestdin.c
	$(CC) -o parsestdin{,.c}
		
gen-chunkmod-filelist-mpi: gen-chunkmod-filelist-mpi.c
	mpicc -o gen-chunkmod-filelist-mpi{,.c}

../../bin/audit-find-between: gen-chunkmod-filelist.py
	cp $^ $@

install: chunkmod_intercept.so
	cp chunkmod_intercept.so /usr/lib64/chunkmod_intercept.so
	./make_beegfs-storage_wrapper.sh

clean:
	find . -name "*.c" -exec basename \{\} .c \; | xargs rm 2> /dev/null ; true

%: %.c
	$(CC) -c -o $@ $< $(CFLAGS)
