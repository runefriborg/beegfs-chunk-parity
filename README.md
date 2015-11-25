beegfs-chunk-parity
===================

This is a delayed raid-5 add-on for the BeeGFS storage system.

Delayed raid-5 means that we provide the same sort of guarantee as a classical
raid-5 system where you can recover from loosing one entire storage target if
you have some catastrophic hardware failure.

The guarantee is not quite as strong as with a usual raid-5 setup though.
It is more like a checkpointing operation, where the guarantee is only valid
for files unchanged since the last time the parity data was updated.
Delayed refers to this delay from when the files are written to when they are
"committed".

We are using this for our own production setup, but so far haven't had any
hardware failures that called for a rebuild and obviously cannot give any
warranties.


Assumptions
===========
Before installing you have to make sure our assumptions are reasonable for
your system.

Some of them are still a bit unfortunate.

* There is only one store per beegfs-storage daemon.
* The one store is labeled as `/store[0-9][0-9]`.
* The beegfs-storage.conf file has the same pattern in its path.

While others are more reasonable.

* Parity config folders are **NOT** in a shared directory.
* The parity config folders are present on all machines.
* The exechost can connect to all other machines with ssh.
* You will be running the parity generation and rebuilding as root.
* There is an `/etc/init.d/beegfs-storage` script similar to the standard one.

Dependencies
------------
The main dependencies are BeeGFS, MPI and LevelDB.

The most important dependency is of course a working BeeGFS setup that you
want to generate parity data for.

The process relies on MPI for all communication both when generating parity
and when restoring a lost storage target from the stored parity data.
You just need a standard MPI install with the `mpicc` and `mpirun` programs
available on the path when compiling and running.

We have developed with MPICH/MVAPICH and recommended that you use that.
OpenMPI seems to want a different format for the list of hosts and a different
set of commandline arguments when running, so you will have to modify the code
`beegfs-parity-{gen,rebuild}` scripts if you want to get that running.

Additionally we use a bunch of programs that should be available on any normal
Linux distribution, like `comm`, `find`, `flock`, `ssh`, etc.


Compile & install
=================
Firstly you will have to modify `src/beegfs-conf.sh` so that it points to your
leveldb include/lib paths (assuming they are not in default system directories)
and to the name of your BeeGFS mount.

Since we test that everyone participating in a run is compiled from the same
git version you might want to commit the changes you make to the config.

The compilation and installation is done by the `build.sh` script.
If you wanted to compile and install to `/opt/beegfs-parity/` it would look
like this:

    ./build.sh build
    PREFIX=/opt/beegfs-parity ./build.sh install

Changelogger
------------
In order to do partial updates of the parity data we need to know what files
have changed between runs. This is done by preloading a library before the
BeeGFS storage service. The library makes sure each thread has a file in
`/dev/shm/` where it logs 'file unlinked' and 'writable file closed' events.
There is no locking and it should have a negligible overall performance impact
on your BeeGFS system since reads and writes are untouched.

The library itself is always compiled and installed, but you have to manually
enable it by calling `$PREFIX/bin/bp-update-storage-wrapper` and restarting
the beegfs storage service.

Enabling it means that the normal BeeGFS storage daemon binary is renamed to
add a .bin extension, while a small script that does the pre-loading is put in
its place.

In our setup we set the `storeStorageDirectory` to either /store01 or /store02,
so the wrapper script is set up to detect these in order to give us simpler
code - you might want to change this in the installed wrapper.  The wrapper has
to set `BP_STORE` to point at the storage directory.


Setup
=====
The parity works on a per-store basis. So if you are using separate stores you
will need to run the parity generation multiple times.
Each of the stores needs a separate folder with configuration parameters and
to write temporary files. This is meant to be a local folder, present at the
same path on all machines, and **not** a NFS share or similar.

Here is how one of these config folders should look:

    /opt/store01-parity-conf/
    |-- etc/
    |   |-- basedir
    |   |-- exechost
    |   `-- hosts
    |-- run/
    `-- spool/


The `basedir` file contains the full path to your store dir, so `/store01` in
our case.

We require the program to be started from the same machine every time and the
hostname of that machine is specified in the `exechost` file. This is mostly
so we can make sure there wont be multiple instances running at the same time
just by using a local file lock.

The `hosts` file contains a list of all your storage targets. Since our setup
has two stores on some machines and only one on others, we check all hosts in
the list if they actually have the relevant store when starting a parity run.
This means you can just use a full list of all the machines that have at least
one storage target.

The `run` folder will hold some files that are only relevant for the one run
(the filtered version of `hosts`, the mpi version of the same etc.), while
`spool` holds more permanent data. Most noticeably the index of where parity
data is stored for each chunk.

Even though the two last folders are empty, you have to make sure they exists
in the config folders on all the storage targets.


Run
===
To generate a full set of parity chunks for the current data you invoke the
`beegfs-parity-gen` tool on the config folder.

    beegfs-parity-gen --complete /opt/store01-parity-conf

This will start the process and give you some progress output showing disk IO
for each storage target. A full run can take a while if your system is large
-- around a day or so on a ~800TB system with 28 storage targets.

The cheaper partial updates can now be done on top of this if the
changelogger is installed. You simply use the `--partial` variation.

    beegfs-parity-gen --partial /opt/store01-parity-conf

You probably want this to run regularly in a cron job. We are running a bit
after midnight every day, which seems to work well for us. It can run as
frequently as you want and the process is locked so you don't have to worry
about overlapping runs screwing up data.

Using the parity data to restore a lost storage target is not fully automated.
The first manual step is to recreate the meta-data files in the store folder.
Specifically we use the `targetNumID` file to recognize who's who.

Once the folder is ready (has meta data + an empty `chunks` folder) you can
rebuild the storage target like this:

    beegfs-parity-rebuild /opt/store01-parity-conf <id>

With the id being the contents of the rebuild targets `targetNumID` file.
It doesn't matter if you rebuild to the same machine or not, as long as you
use the same id and of course you have to make sure BeeGFS uses the right
machine too.

When the target is restored you should be able to bring BeeGFS online again,
and any file that was untouched between the last parity generation and the
crash will have been restored to its old state.
Anything changed after the most recent run will be broken to some degree and
we can't really tell how much.

The last safe time is available as a POSIX timestamp via the
`spool/last-gen-timestamp` file in the config folder. You can either manually
mark anything modified after this time or you can use the `bp-set-corrupt`
tool which also takes in to account whether the files might have chunks on the
affected target.
