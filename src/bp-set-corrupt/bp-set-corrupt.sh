#!/bin/sh


PID=$$

# Args
DIR=""
CACHEDIR="."
TIMESTAMP=""
TARGETID=""
BINDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# Load configuration
. ${BINDIR}/beegfs-conf.sh

failed_params=0

# Parse params
while getopts "d:c:s:t:h" opt; do
        case $opt in
                d) DIR=$OPTARG ;;
                c) CACHEDIR=$OPTARG ;;
                s) TIMESTAMP=$OPTARG ;;
                t) TARGETID=$OPTARG ;;
                h) failed_params=1 ;;
                *) failed_params=1 ;;
        esac
done


# Check params
if [ failed_params == 1 ] || [ $# -eq 0 ] || [ "$DIR" == "" ] || [ "$TIMESTAMP" == "" ] || [ "$TARGETID" == "" ]; then
        cat <<EOF
Usage:
  bp-set-corrupt -d <directory to update> [-c <cache directory>] -s <timestamp> -t <storage target ID>

This is used to identify all files which may be corrupt, after a rebuild of a storage target.

Files, which are identified, are renamed to FILENAME.CORRUPT

Parameters explained:
  -d    Directory located on a BeeGFS mount
  -c    Cache directory used for large temporary files (not located in BeeGFS mount!)
          default cache directory = "."
  -s    Timestamp of the last parity-update used for rebuilding a storage target
  -t    Storage target Numeric ID (example: 292.01)
EOF
        exit 1
fi

PWD=`pwd`

echo "##### ${DIR} #####"

# Change to CACHEDIR
cd ${CACHEDIR}

echo "Creating file lists ${CACHEDIR}/output.*"

# Create file lists
${BINDIR}/bp-cm-filelist ${#CONF_BEEGFS_MOUNT} "${DIR}" ${TIMESTAMP}

if [ ! "$?" -eq 0 ]; then
        echo "Aborted! Check system consistency!"
        exit 1
fi

# Count number of file entries
FILES=$(cat output.* | wc -l)

echo "Total count: ${FILES}"

echo "## Rename files to FILENAME.CORRUPT if located on ${TARGETID} ##"


# Get entries and rename if possibly corrupt
${BINDIR}/bp-cm-getentry ${CONF_BEEGFS_MOUNT} ${FILES} ${TARGETID}

if [ ! "$?" -eq 0 ]; then
        echo "Aborted! Check system consistency!"
        exit 1
fi

# Change to PWD
cd ${PWD}

# Cleanup
rm -f ${CACHEDIR}/output.*

