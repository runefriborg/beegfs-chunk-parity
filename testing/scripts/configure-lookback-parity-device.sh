#!/bin/sh

# Abort if loopfile exists
if [ -f /store01/loopfile ]; then
	echo "Aborted: /store01/loopfile exists!"
	exit 1
fi


# Create 10 GB file
dd if=/dev/zero of=/store01/loopfile bs=10M count=1000

# Format loopback file
mkfs.xfs /store01/loopfile

# Create mount point
mkdir -p /store01/parity

# Mount
mount -o loop -t xfs /store01/loopfile /store01/parity

