Beegfs-chunkmodlogger
=====================

In the quest for adding redundancy into beegfs it is nesessary to keep a log
of which chunk has been modified and when. This will give a list af chunk whichs
paraty is out of sync and will have to be updated in the next paraty-comp-run.

It is possible to log all open-calls made by beegfs without making changes to
the source code via LD_PRELOAD. `chunkmod_intercept` does this verbatimly. 

`gen-chunkmod-filelist.py` will do the post-prosessing of the logs from
`chunkmod_intercept` and generate a list of modies chunk to be read by the
paraty-run.

`gen-full-filelist.sh` generates the list for the initial paraty-run. A list of
all chunks.

Setting up the logger
=====================

To install the logger module:

make
make install 

To inject the logger into beegfs modify its init.d-script and add:

"LD_PRELOAD=/usr/lib64/chunkmod_filelist.so"


Chunkmods will be logged to `/dev/shm/chunkmod_intercept/`

Run `gen-chunkmod-filelist.py -f 0 -t 9999999999 | parsestdin` to parse and
print  all logged so far.


