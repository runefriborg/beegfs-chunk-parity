# Settings included (sourced) from beegfs scripts and from Make files

# LevelDB dependency
CONF_LEVELDB_LIBPATH="/project/SystemWork/leveldb"
CONF_LEVELDB_INCLUDEPATH="/project/SystemWork/leveldb/include"

# Settings
CONF_BEEGFS_MOUNT="/faststorage"

# MAX_ITEMS should be at least the total number of expected logical files in
# your BeeGFS system divided by the number of storage targets plus some safety
# margin.
# For our system with 28 storage targets, MAX_ITEMS=25M will allow roughly
# 700M files and use around 4GB of memory + additional memory for a hash table
# that is dependent on the actual number of files seen.
MAX_ITEMS=25000000ULL
