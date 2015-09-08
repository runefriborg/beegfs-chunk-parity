bp-changelogger.so: changelogger.c
	$(CC) -fPIC -Wall -shared -o $@ $^ -ldl

parsestdin: parsestdin.c
	$(CC) -o parsestdin{,.c}

install: bp-changelogger.so
	cp $^ /usr/lib64/$^
	./make_beegfs-storage_wrapper.sh
	cp gen-chunkmod-filelist.py ../../bin/bp-find-chunks-changed-between

clean:
	rm -f bp-changelogger.so
	rm -f parsestdin.o
