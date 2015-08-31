#!/usr/bin/python2
# -*- coding: utf-8 -*-

import os
import sys
import time
import struct
import optparse

chunks_modified = {}

FILEMOD_PATH = "/dev/shm/chunkmod_intercept"

def get_files(path,lowerbound,upperbound):
    f = []
    for (dirpath, dirnames, filenames) in os.walk(path):
        f.extend(filenames)
        break

    retlist = []
    for file in f:
        try:
            stat = os.stat(os.path.join(path,file))
        except:
            continue # TODO Ignoring unlinks at this point

        # if st_mtime < lowerbound => All entries to old
        # if st_ctime > upperbound => All entries to new
        if stat.st_ctime > upperbound or stat.st_mtime < lowerbound:
            continue

        retlist.append(os.path.join(path,file))

    return retlist

def write_to_stdout():
    ''' Convert entry into easy C parseble types.
        * 8-byte unsigned int for timestamp followed by
        * 8-byte char's contaning either 'd' or 'm' (delete and modify). Must
          be padded to fit nicely into the parseble format
        * 8-byte unsigned int denoting the lenght of the coming string
        * The string itself

        Repeat this pattern without any seperators.

        Write output to stdout.
    '''
    for path,(timestamp,type,size) in chunks_modified.items():
        time_b = struct.pack('<Q',int(timestamp))
        size_b = struct.pack('<Q',int(size))
        len_b  = struct.pack('<Q',len(path))
        assert len(type) is 1
        type_b  = struct.pack('<Q',long(ord(type)))

        sys.stdout.write(time_b+size_b+type_b+len_b+path+'\0')

def insert(entries):
    ''' Weed out all but the newest entry for each chunk.
        If timestamp_1 == timestamp_2, type='d' takes precedent.'''

    for timestamp,type,path,size in entries:
        if path in chunks_modified.keys() and timestamp < chunks_modified[path][0]:
            continue
        elif path in chunks_modified.keys() and timestamp is chunks_modified[path][0] and type is not 'd':
            continue
        else:
            chunks_modified[path] = (timestamp,type,size)

def parse(file,store,lowerbound,upperbound):
    ''' Takes a string as input and parses it into tuples of (timestamp, type,
        path). We exploite the following:
        - One log file pr. thread. This will ensure
          that input is sorted by timestamp
    '''

    valid_entries = []
    for entry in file:
        timestamp,type,path = entry.strip().split(' ')
        timestamp = int(timestamp)
        if path[:len(store)] == store and timestamp < upperbound and timestamp > lowerbound:
            if type != 'd':
                try:
                    stat = os.stat(os.path.join(path))
                    size = stat.st_size
                except:
                    size = 0
            else:
                size = 0
            valid_entries.append((timestamp,type,path,size))

    return valid_entries


def construct_chunkmod_data(path,store,lowerbound,upperbound):
    files = get_files(path,lowerbound,upperbound)
    for filename in files:
        with open(filename,'r') as file:
            entries = parse(file,store,lowerbound,upperbound)
            insert(entries)

    write_to_stdout()

def cleanup_until(until):
    ''' When a log entry is being written it will update the st_mtime of a log
    file. To gather all relevant log files contaning log entries up until a
    given point in time, can therefore be done only by looking at the st_mtime
    of the logfiles themself. '''

    #Get all files whichs stat.st_mtime is less than until
    filelist = get_files(FILEMOD_PATH,0,until)
    for file in filelist:
        print file
        #os.remove(file)


def main():
    parser = optparse.OptionParser(usage="""Usage: %prog [--from <timestamp> --to <timestamp>|--cleanup-until <timestamp>] --stote <store prefix>"

%prog costructs a list of files that have been modified within the given time
interval. It does this by looking at the logs files generated by a LD_PRELOAD'ed
chunkmod module to beegfs-storage.

The output format is constructed to be easy parserable in C.
Format: <time_t><type><len><str>
    time_t: 8 byte unsigned int contaning timestamp of chunkmod.
    type: a char packed into 8 bytes, to make the parsing more clean
    len: 8 byte unsigned int denoting the lenght of the coming string
    str: char array of the above lenght contating the path of the file.

    note: no NULL byte at the end af str

Filemod log that has been processed can be removed by via --cleanup-until.""",
                                   version="%prog 1.0")
    parser.add_option("-f", "--from" , dest="from_t", type="int",
                      help="Include changes made 'from_t'", metavar="TIMESTAMP")
    parser.add_option("-t", "--to"   , dest="to_t"  , type="int",
                      help="Include changes made until 'to_t'", metavar="TIMESTAMP")
    parser.add_option("-c", "--cleanup-until", dest="clean_t", type="int",
                      help="Delete chunkmod logs until given timestamp.", metavar="TIMESTAMP")
    parser.add_option("-s", "--store", dest="store"  , type="string",
                      help="Store to work on", metavar="STORE")

    (options, args) = parser.parse_args()

    if options.from_t is not None and options.to_t is not None and options.store is not None:
        if options.store[0] != "/":
            options.store = "/"+options.store
        construct_chunkmod_data(FILEMOD_PATH,options.store,options.from_t,options.to_t)

    elif options.clean_t is not None:
        cleanup_until(options.clean_t)
    else:
        print parser.usage


if __name__ == "__main__":
    main()
