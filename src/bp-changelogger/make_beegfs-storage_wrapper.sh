#!/bin/bash

if [ ! -f /etc/init.d/beegfs-storage ]; then
    echo "Skipping beegfs-storage wrapper"
    exit 0
fi

# Get the binary from the init.d-script
SERVICE_NAME=$(grep SERVICE_NAME= /etc/init.d/beegfs-storage | cut -d= -f 2)
APP_BIN=$(grep APP_BIN=      /etc/init.d/beegfs-storage | cut -d= -f 2)
BEEGFS_STORAGE_BIN=$(eval echo $APP_BIN)

# If the wrapper already exists, exit
(file "${BEEGFS_STORAGE_BIN}" | grep -q ELF) && mv "$BEEGFS_STORAGE_BIN" "$BEEGFS_STORAGE_BIN.bin"

cat > "$BEEGFS_STORAGE_BIN" << EOF
#!/bin/bash

if [[ "\$@" =~ store[0-9][0-9] ]]; then
    BP_STORE="\${BASH_REMATCH}"
else
    echo "** ERROR: Cannot tell which store this instance is using" 1>&2
    exit 1
fi

export BP_STORE
LD_PRELOAD="/usr/lib64/bp-changelogger.so" exec -a "\$0" "\$0.bin" \$@
EOF

chmod +x "$BEEGFS_STORAGE_BIN"
