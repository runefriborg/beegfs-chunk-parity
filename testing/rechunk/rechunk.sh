#!/bin/bash

TMPFILE=$1
UUID=$2
LOC=$3

if [ "$LOC" == "" ]; then
	echo "Usage rewrite.sh <tmp file - located on shared storage!> <uuid> <path to rewrite!>"
	exit 1
fi

echo -n "Creating file with commands..."
find ${LOC} -size +10M -type f \! -name '*.UUID.*' -exec echo rsync -q -a '{}' '{}.UUID.'${UUID} '&& cmp {} {}.UUID.'${UUID} '&& mv -f {}.UUID.'${UUID} '{}' '|| echo ERROR: {}' ';' > $TMPFILE
echo "done"

echo "Use this command to run using dispatch:"

