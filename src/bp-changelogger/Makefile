default: bp-changelogger.so parsestdin

bp-changelogger.so: changelogger.c
	$(CC) -fPIC -Wall -O2 -shared -o $@ $^ -ldl

parsestdin: parsestdin.c
	$(CC) -o parsestdin{,.c}

clean:
	rm -f bp-changelogger.so
	rm -f parsestdin
