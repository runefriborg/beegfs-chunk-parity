#!/bin/sh


PID=$$

# Args
DIR=""
LEVELDB=""
CACHEDIR="."
BINDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# Load configuration
. ${BINDIR}/beegfs-conf.sh

failed_params=0

# Parse params
while getopts "d:o:c:h" opt; do
        case $opt in
                d) DIR=$OPTARG ;;
                o) LEVELDB=$OPTARG ;;
                c) CACHEDIR=$OPTARG ;;
                h) failed_params=1 ;;
                *) failed_params=1 ;;
        esac
done


# Check params
if [ failed_params == 1 ] || [ $# -eq 0 ] || [ "$DIR" == "" ] || [ "$LEVELDB" == "" ]; then
        cat <<EOF
Usage:
  bp-chunkmap -d <directory to map> -o <leveldb output file> [-c <cache directory>]

Parameters explained:
  -d    Directory located on a BeeGFS mount
  -o    Output file containing a chunk map in leveldb format (not located in BeeGFS mount!)
  -c    Cache directory used for large temporary files (not located in BeeGFS mount!)
          default cache directory = "."
EOF
        exit 1
fi

PWD=`pwd`

# Get path for LEVELDB
if [ ! "${LEVELDB:0:1}" == "/" ]; then
        LEVELDB="${PWD}/${LEVELDB}"
fi

echo "##### ${DIR} #####"

# Change to CACHEDIR
cd ${CACHEDIR}

echo "Creating file lists ${CACHEDIR}/output.*"

# Create file lists
${BINDIR}/bp-cm-filelist ${#CONF_BEEGFS_MOUNT} "${DIR}"

if [ ! "$?" -eq 0 ]; then
        echo "Aborted! Check system consistency!"
        exit 1
fi

# Count number of file entries
FILES=$(cat output.* | wc -l)

echo "Total count: ${FILES}"

echo "Write entries to LevelDB: ${LEVELDB}"

# Get entries and create db
LD_LIBRARY_PATH=${CONF_LEVELDB_LIBPATH} ${BINDIR}/bp-cm-getentry ${FILES} ${LEVELDB}

if [ ! "$?" -eq 0 ]; then
        echo "Aborted! Check system consistency!"
        exit 1
fi

# Change to PWD
cd ${PWD}

# Cleanup
rm -f ${CACHEDIR}/output.*

