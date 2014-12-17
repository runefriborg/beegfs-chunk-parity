Every rank starts two threads: a scanner and a coordinator.

Scanner
-------
Scan every file in /store01 in sorted order.
For every file a local coordinator(*co*) is chosen based on the filename.
The message (filename, my rank, last modified, chunk size) is sent to *co*.

Coordinator
-----------
Receives chunk info messages from whoever sends them to us until all scans are
done.

Given our persistent database we can see which pieces of information are
relevant.
If the set of ranks that have information about a file has changed since the
database was updated, if any of the ranks report a newer modified date or if
the file isn't mentioned in the database we need to (re)process the file.

Every coordinator thread build a tasklist containing something like:
`(rank1,..,rank8, rankP,rankQ, most recent 'last modified' field, largest size)`
Then each coordinator in turn broadcasts their tasklist and everyone processes
it in parallel.

    for r = 0 to max_rank:
      if r == my_rank:
        current_tasks = broadcast_send(my_tasks)
      else:
        current_tasks = broadcast_receive()
      process_tasks(current_tasks)
      sync()

Actually processing files is done in parallel as described in `tasks.md` and
the prototype implementation in `src/mpi-tasklist-test.c` except that the
persistent databases are updated along the way.

Persistent database
===================
This is a very simple database (like LevelDB) that maps a filename to a small
amount of information.
We store which nodes had chunks, which nodes were chosen for P/Q and when the
parity information was generated.

Most of the time we will need to index by filename, but when a storage target
has died we will also need to iterate all keys and values in the database to
find files that had a chunk on the dead storage target.

Not all storage targets need to store the full database. The coordinators need
to store information for every filename they are coordinating so they can check
if a parity calculation is necessary. And if the dead target is the coordinator
we need to have at least one copy somewhere else. The simplest solution would
be to just store it in the databases of everyone involved (storage target 1..8,
P, Q and the coordinator).
