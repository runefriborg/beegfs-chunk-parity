#!/bin/bash

TMPFILE=$1
UUID=$2
LOC=$3

if [ "$LOC" == "" ] || [ "$TMPFILE" == "" ] || [ "$UUID" == "" ]; then
	echo "Usage rechunk.sh <tmp file - located on shared storage!> <uuid> <path to rechunk!>"
	exit 1
fi

echo -n "Creating file with commands..."
find ${LOC} -size +10M -type f \! -name '*.UUID.*' -exec echo cp -a '"{}"' '"{}.UUID.'${UUID}'"' '&& cmp "{}" "{}.UUID.'${UUID}'"' '&& mv -f "{}.UUID.'${UUID}'"' '"{}"' '|| echo "ERROR: {}"' ';' > $TMPFILE

echo "done"

echo "Use this command to run using dispatch:"
echo sbatch -N 8 --ntasks-per-node=1 --mem=1G dispatch -r ${TMPFILE} 

