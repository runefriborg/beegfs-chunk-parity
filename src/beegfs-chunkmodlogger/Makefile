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

init: beegfs-storage.init-d
	diff $^ /etc/init.d/beegfs-storage  -U 0 | grep @@ | wc -l | grep 1 -q && diff beegfs-storage.init-d /etc/init.d/beegfs-storage | grep ">.*daemon" -q && echo "Replaceing beegfs-storage.init-d" && \cp $^ /etc/init.d/beegfs-storage -v

clean:
	find . -name "*.c" -exec basename \{\} .c \; | xargs rm 2> /dev/null ; true

%: %.c
	$(CC) -c -o $@ $< $(CFLAGS)
