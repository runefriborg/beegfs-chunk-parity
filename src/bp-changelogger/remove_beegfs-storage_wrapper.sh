#!/bin/bash

if [ ! -f /etc/init.d/beegfs-storage ]; then
    echo "Skipping beegfs-storage wrapper"
    exit 0
fi

# Get the binary from the init.d-script
SERVICE_NAME=$(grep SERVICE_NAME= /etc/init.d/beegfs-storage | cut -d= -f 2)
APP_BIN=$(grep APP_BIN=      /etc/init.d/beegfs-storage | cut -d= -f 2)
BEEGFS_STORAGE_BIN=$(eval echo $APP_BIN)

(file "${BEEGFS_STORAGE_BIN}.bin" | grep -q ELF) && mv "$BEEGFS_STORAGE_BIN.bin" "$BEEGFS_STORAGE_BIN"
