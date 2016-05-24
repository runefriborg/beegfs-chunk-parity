#!/bin/bash

TARGETGREP=$(echo $1 | tr ',' '\n' | awk '{print "-e ID:."$1"";}' | tr '\n' ' ')
TMPFILE=$2
UUID=$3
LOC=$4

if [ "$LOC" == "" ] || [ "$TMPFILE" == "" ] || [ "$UUID" == "" ]; then
	echo "Usage rechunk.sh <comma-separated prefix target IDs> <tmp file - located on shared storage!> <uuid> <path to rechunk!>"
	echo
	echo "Example:"
	echo "./rechunk-specific-targets.sh 291,292 rewrite-commands XYZABC /faststorage/home/runef/"
	exit 1
fi

FILELIST=$(mktemp)

echo -n "Creating filelist $FILELIST..."
for filename in `find ${LOC} -size +10M -type f \! -name '*.UUID.*'`; do 
	if beegfs-ctl --getentryinfo $filename | grep -q $TARGETGREP
	then
		echo $filename >> $FILELIST
	fi
done
echo done

echo -n "Creating file with commands..."

for x in `cat $FILELIST`; do
	find $x -exec echo cp -a '"{}"' '"{}.UUID.'${UUID}'"' '&& cmp "{}" "{}.UUID.'${UUID}'"' '&& mv -f "{}.UUID.'${UUID}'"' '"{}"' '|| echo "ERROR: {}"' ';'
done > $TMPFILE

echo "done"

echo "Use this command to run using dispatch:"
echo sbatch -N 8 --ntasks-per-node=1 --mem=1G dispatch -r ${TMPFILE} 

