bp-changelogger
===============

In the quest for adding redundancy into beegfs it is nesessary to keep a log
of which chunk has been modified and when. This will give a list af chunk whichs
parity is out of sync and will have to be updated in the next parity-comp-run.

It is possible to log all open-calls made by beegfs without making changes to
the source code via LD_PRELOAD. `bp-changelogger.so` does this verbatimly.

`gen-chunkmod-filelist.py` will do the post-prosessing of the logs from
`bp-changelogger.so` and generate a list of modies chunk to be read by the
parity-run.

Setting up the logger
=====================

To install the logger module:

`make install`

The `beegfs-storage` executable will be replaced with a wrapper that preloads
the correct shared object.

Chunkmods will be logged to `/dev/shm/beegfs-changelog/`

Run `gen-chunkmod-filelist.py -f 0 -t 9999999999 | parsestdin` to parse and
print  all logged so far.

