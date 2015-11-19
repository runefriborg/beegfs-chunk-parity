#!/bin/bash
set -e

CC=gcc
MPICC=mpicc
BASEDIR="$(cd $(dirname ${BASH_SOURCE[0]}); pwd)"
BUILD="$BASEDIR/build"
CPPFLAGS=${CPPFLAGS:--Wall -Wextra -std=gnu99 -Os}
PREFIX="$(cd ${PREFIX:-$BASEDIR}; pwd)"

source src/beegfs-conf.sh

# compiles with default flags and output(1st arg) in the BUILD dir.
_cc() {
    local target=$1
    shift
    $CC $CPPFLAGS -o "$BUILD/$target" $@
}

# compiles with default flags and output(1st arg) in the BUILD dir.
_mpicc() {
    local target=$1
    shift
    $MPICC $CPPFLAGS -o "$BUILD/$target" $@
}

build() {
    ( cd "src/bp-changelogger"
    $CC -fPIC -Wall -O2 -shared -o "$BUILD/bp-changelogger.so" changelogger.c -ldl
    cp gen-chunkmod-filelist.py "$BUILD/bp-find-chunks-changed-between"
    )

    ( cd "src/bp-set-corrupt/"
    _cc bp-cm-filelist filelist-runner.c -lpthread
    _cc bp-cm-getentry getentry-runner.c -lpthread
    cp bp-set-corrupt.sh "$BUILD/bp-set-corrupt"
    )

    ( cd "src/bp-find-all-chunks/"
    _cc bp-find-all-chunks main.c
    )

    ( cd "src/beegfs-raid5"
    GIT_COMMIT=0x$(git rev-parse --short=8 HEAD 2>/dev/null || echo DEADBEEF)
    if [ "$GIT_COMMIT" == "0xDEADBEEF" ]; then
        echo "Warning: Not a git repository, can't set a proper version" >&2
    fi
    local CPPFLAGS="${CPPFLAGS} -I${CONF_LEVELDB_INCLUDEPATH} -D_GIT_COMMIT=${GIT_COMMIT}"
    local lvldb="-L${CONF_LEVELDB_LIBPATH} -lleveldb"
    local common="$BUILD/progress_reporting.o $BUILD/task_processing.o $BUILD/persistent_db.o"

    _mpicc progress_reporting.o -c common/progress_reporting.c
    _mpicc task_processing.o    -c common/task_processing.c
    _mpicc persistent_db.o      -c common/persistent_db.c

    _mpicc bp-parity-gen     gen/main.c gen/file_info_hash.c gen/assign_lanes.c $common $lvldb -DMAX_WORKITEMS=$MAX_ITEMS
    _mpicc bp-parity-rebuild rebuild/main.c                                     $common $lvldb
    )

    cp "src/beegfs-parity-gen"      "$BUILD/"
    cp "src/beegfs-parity-rebuild"  "$BUILD/"
}

clean() {
    find "$BUILD" -mindepth 1 -delete
}

install_bins() {
    ( cd $BUILD
    mkdir -p "$PREFIX/lib64"
    mkdir -p "$PREFIX/bin"
    rm -f "$PREFIX/lib64/bp-changelogger.so"
    cp bp-changelogger.so   "$PREFIX/lib64/"
    cp bp-cm-*              "$PREFIX/bin/"
    cp bp-find-*            "$PREFIX/bin/"
    cp bp-parity-*          "$PREFIX/bin/"
    cp bp-set-corrupt       "$PREFIX/bin/"
    cp beegfs-parity-*      "$PREFIX/bin/"
    )
}

enable_logger() {
    ( cd "src/bp-changelogger/"
    PREFIX=$PREFIX ./make_beegfs-storage_wrapper.sh
    echo "Change-logging will be enabled when you (re)start the BeeGFS storage service"
    )
}

disable_logger() {
    ( cd "src/bp-changelogger/"
    PREFIX=$PREFIX ./remove_beegfs-storage_wrapper.sh
    echo "Change-logging will be disabled when you (re)start the BeeGFS storage service"
    )
}

cd $BASEDIR

case $1 in
    build)
        mkdir -p "$BUILD"
        build
    ;;
    install) install_bins $PREFIX ;;
    enable-log) enable_logger $PREFIX ;;
    disable-log) disable_logger $PREFIX ;;
    clean) clean ;;
    *) echo help ;;
esac
