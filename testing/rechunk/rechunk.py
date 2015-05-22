#!/usr/bin/env python2

import argparse
import platform
import os.path
import os
import signal
import random
import sys
import subprocess
import threading
from math import log10, ceil

from time import sleep
import Queue


"""
Processes started through SSH write to stdout every second. If the SSH is
closed, then we get a SIGPIPE exception, which the worker interprets as a
stop signal.
"""

# TODO
# See if we can handle worker failures, and at least prevent them.
#
# Handle if we cannot read the directory, and make sure we only try to rewrite
# files we have permission to rewrite.
#
# Figure out where we should look up our client side file


###############################################################################
######################### CONFIGURATION OPTIONS ###############################
###############################################################################

# How many workers per node in nodefile.
multiplier=1

###############################################################################
####################### PARENT MODE FUNCTIONS #################################
###############################################################################

l = threading.Lock()
cumsum = 0 # Cumulative copied files
tsum = 0   # Total files
run = True

class Worker:
    def __init__(self, hostname, count, fh):
        self.hostname = hostname
        self.count = count
        self.fh = fh


def check_folder(path):
    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError('{0} is not a folder'.format(path))
    return path


def rewrite_worker(worker, idx, path, cachedir):
    global cumsum

    invocation = './Projects/beegfs-chunk-parity/testing/rechunk/rechunk.py {} -w -f {} -c {} --id {}'.format(path, worker.listname, cachedir, idx)
    cmd = ['ssh', worker.hostname, invocation]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    prev = 0

    for line in iter(p.stdout.readline, ''):
        # Split by the first ','
        sep = line.split(',', 1)

        count = int(sep[0])
        diff = count - prev

        if diff > 0:
           l.acquire()
           cumsum += diff
           l.release()

        prev = count

        if p.returncode != None:
            break

    return


def read_nodefile(f):
    nodes = f.readlines()
    nodes = map(lambda n: n.strip(), nodes)
    f.close()
    return nodes


# Create temporary directory, assumes that it is already inside path.
def create_temporary_directory(path, cachedir):
    cachedir = os.path.normpath(cachedir)

    if not os.path.isabs(cachedir):
        cachedir = os.path.abspath(cachedir)

    if not os.path.exists(cachedir):
        try:
            os.mkdir(cachedir)
        except OSError as e:
            print "Unable to create cachedir:", e.strerror
            return None
            

    if not os.path.isdir(cachedir):
        print "Error: '{0}' is not a directory".format(cachedir)
        return None


    files = os.listdir(cachedir)

    if len(files) != 0:
        print "Error: Cache directory not empty ({})".format(cachedir)
        return None

    return cachedir


def create_file_lists(path, workers):
    cmd = ['find', '.', '-type', 'f', '-size', '+8M']
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    random.seed()
    s = len(workers)

    global tsum

    for line in iter(p.stdout.readline, ''):
        idx = random.randrange(0, s)
        workers[idx].fh.write(line)
        workers[idx].count += 1

        tsum += 1

    p.wait()
    if p.returncode != 0:
        return False

    return True


def async_notifier(interval):
    width = int(ceil(log10(tsum)))
    while run:
        print "Rechunking {cumsum:>{width}}/{tsum} files".format(cumsum=cumsum, tsum=tsum, width=width)
        sleep(interval)


def sigint_handler(signal, frame):
    pass


def main(path, nodefh, interval, cachedir):
    global count

    nodes = read_nodefile(nodefh)

    workers = []

    path = os.path.abspath(path)

    os.chdir(path)

    # Set up temporary directory
    cachedir = create_temporary_directory(path, cachedir)
    if cachedir == None:
        return

    print "Generating file lists for workers..."

    for node in nodes:
        for i in range(multiplier):
            listname = '{0}/{1}.{2}.list'.format(cachedir, node, i)
            fh = open(listname, 'w')
            w = Worker(node, 0, fh)
            w.listname = listname

            workers.append(w)

    # Stream a list of files (find) into python, where for each file, you
    # add it to one of the worker filelists, incrementing it's size
    # every time a file is added.


    # This populates '.listname' for each worker, in their respictive files.
    try:
        if not create_file_lists(path, workers):
            print "Error generating file lists"
            return
    finally:
        for worker in workers:
            worker.fh.close()


    threads = []
    idx = 0

    print "Starting Workers..."

    # Start workers, one per file (using ssh)
    for worker in workers:
        args = {
                "worker": worker,
                "idx": idx,
                "path": path,
                "cachedir": cachedir
                }
        idx += 1

        t = threading.Thread(target=rewrite_worker, kwargs=args)
        threads.append(t)

        t.daemon = True
        t.start()

    async = threading.Thread(target=async_notifier, kwargs={"interval": interval})
    async.daemon = True
    async.start()

    for i in threads:
        i.join()

    # Threads have shut down, clean the working directory.

    files = os.listdir(cachedir)
    for f in files:
        os.unlink("{}/{}".format(cachedir, f))

    os.rmdir(cachedir)

    # THIS IS THE END


###############################################################################
####################### WORKER MODE FUNCTIONS #################################
###############################################################################

curfile = ""
curidx  = 0
interrupted = False

# Reports progress to stdout every interval. The main thread is listening over
# ssh.
def worker_async_reporter(interval):
    while run:
        sleep(interval)
        if curfile != "":
            sys.stdout.write("{0},{1}\n".format(curidx, curfile))
            sys.stdout.flush()


# Try to catch sigint signals.
def sigpipe_handler(signal, frame):
    global interrupted
    global run
    run = False
    interrupted = True

    #fh = open('/home/kse/log/{}'.format(platform.node()), 'w')
    #fh.write("Killed by {}".format(signal))
    #fh.close()

def worker_func(path, filelist, cachedir, idx):
    if not os.path.isabs(filelist):
        print "Error: Path to filelist must be absolute."

    # Go into the directory we are working in
    os.chdir(path)

    # These are used to notify a different worker about what we a copying.
    global curfile
    global curidx

    #signal.signal(signal.SIGINT, sigint_handler)

    # These workers regularly print stuff on stdout, and when SSH connection
    # is lost, we get a sigpipe error. Interpret this as a stopping signal.
    signal.signal(signal.SIGPIPE, sigpipe_handler)

    # Start the thread that prints the file we are working on to STDOUT.
    async = threading.Thread(target=worker_async_reporter, kwargs={"interval": 1})
    async.daemon = True
    async.start()

    with open(filelist) as fp:
        for i in iter(fp.readline, ''):
            i = i.rstrip() # Remove newline after filename

            # Let the reporter thread know what we are doing.
            curfile =  i
            curidx  += 1


            # The name of the temporary file.
            #tempname = "{}/.rechunk.{}".format(cachedir, os.path.basename(i))
            tempname = cachedir + "/.rechunk." + str(idx) + "." + os.path.basename(i)
            tempname = os.path.normpath(tempname)

            p = subprocess.Popen(['cp', '-p', '-f', i, tempname])


            # Loop while checking if copying is done or we have been
            # interrupted.  If we have then we need to stop the program.
            while True:
                r = p.poll()

                if interrupted:
                    if r == None:
                        p.kill()

                    fp.close()
                    os.unlink(filelist)
                    os.unlink(tempname)

                    return

                if r != None and r != 0:
                    print "Unable to copy file: " + str(p.returncode)
                    return

                if r == 0:
                    break

                sleep(0.01)


            # Interrupted is set by the sigint handler.
            if interrupted:
                break

            p = subprocess.Popen(['mv', '-f', tempname, i])

            # Wait for copy to complete.
            while True:
                r = p.poll()

                # DO NOT interrupt a move. It should be fast anyway.
                #if interrupted:
                #    if r == None:
                #        p.kill()

                #    # TODO: Cleanup?
                #    return

                if r != None and r != 0:
                    print "Unable to move file: " + str(p.returncode)
                    return

                if r == 0:
                    break

                sleep(0.01)

            # Interrupted is set by the sigint handler.
            if interrupted:
                fp.close()
                os.unlink(filelist)
                break

    return


if __name__ == '__main__':
    global verbose
    parser = argparse.ArgumentParser()


    # For master mode
    parser.add_argument('-n', '--nodefile', type=argparse.FileType('r'),
            help='Start workers on all nodes in NODEFILE. Default is to look in $PBS_NODEFILE.')

    parser.add_argument('-i', '--interval', type=int,
            help='Progress reporting interval', default=3)
    parser.add_argument('-c', '--cachedir', type=str,
            help='Directory in which to store cache', default='.rechunk')

    parser.add_argument('path',  type=check_folder, help='rechunk path')


    # For worker mode
    parser.add_argument('-f', '--filelist', type=str, help='file containing filenames to be rewritten')
    parser.add_argument('-w', '--worker', action='store_true', help='worker mode (do not call manually)')
    parser.add_argument('--id', type=int, help='ID to make sure we have unique filenames')


    d = parser.parse_args()

    if d.worker:
        if d.filelist != None:
            worker_func(d.path, d.filelist, d.cachedir, d.id)
        else:
            print "Missing filelist"
    else:

        if not d.nodefile:
            nf = os.getenv('PBS_NODEFILE')
            if nf == None:
                print "No nodefile given and PBS_NODEFILE is not set"
                sys.exit(1)

            d.nodefile = open(nf, 'r')

        try:
            main(d.path, d.nodefile, d.interval, d.cachedir)
        except KeyboardInterrupt:
            print "Interrupted by SIGINT"
