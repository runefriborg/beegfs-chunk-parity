CC=gcc
CFLAGS=-I.

chunkmod_intercept: chunkmod_intercept.c
	$(CC) -fPIC -Wall -shared -o chunkmod_intercept.{so,c}  -ldl

parsestdin: parsestdin.c
	$(CC) -o parsestdin{,.c}


install: chunkmod_intercept.so
	cp chunkmod_intercept.so /usr/lib64/chunkmod_intercept.so

clean:
	find . -name "*.c" -exec basename \{\} .c \; | xargs rm 2> /dev/null ; true
	find . -name "*.so" -exec basename \{\} .so \; | xargs rm 2> /dev/null ; true

%: %.c
	$(CC) -c -o $@ $< $(CFLAGS)
