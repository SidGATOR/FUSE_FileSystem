#!/usr/bin/env python

# Group members:
# Suvrat Tedia
# Sidhharth Sharma



"""
Description:
RPC Corrupt APR call for corrupting data in the data server
    The API allows corruption of data single or multiple data servers.
    The API takes in 4 types of arguments namely;
     1) Single/Multi Corrupt
     2) Meta server Port
     3) Data server Ports
     4) File Name (Note use '/' before the file name if the file is in the root of the mount point of FUSE system)

    The API has the following syntax
     python corrupt_data.py <No of corrupt server (1/2)> <meta server port> <data server ports> ... </filename>

    Example for 3 servers for single corrupt server
     python corrupt_data.py 1 2222 3333 4444 5555 /filename.txt

    Example for 4 servers for multi corrupt server (for non adjacent server)
     python corrupt_data.py 2 2222 3333 4444 5555 6666 /filename.txt
"""


import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest, hashlib, os.path,shelve,errno,random
from datetime import datetime, timedelta
from xmlrpclib import Binary
from socket import error as socket_error

block_size = 8

def get(path):
    return pickle.loads(metaserv.get(Binary(path)).data)

def corrupt(path):
    phash = hash_(path)
    parent = path[:path.rfind('/')]  # key for parent
    if len(parent) == 0:
        parent = '/'
    meta = get(parent)
    try:
        file_size = meta[path]["st_size"]
    except KeyError:
	print "File Not found"
	return
    total_blocks = (file_size-1) // block_size + 1
    if total_blocks > 2:
       blk = random.randint(0, total_blocks - 1 - 2)
    else:
       blk = random.randint(0,total_blocks-1)
    ack1 = False
    ack2 = False
    ack1 = dataserv[(phash + blk) % len(dataserv)].corrupt_data(Binary(str(blk) + path))
    if corruptNUM == str(2) and total_blocks > 2:
      ack2 = dataserv[(phash + blk + 2) % len(dataserv)].corrupt_data(Binary(str(blk+2) + path))
      if ack1 and ack2 == True:
        print "Corrupted block number: %d and %d of file: %s. Assoicated Server number are %d and %d:" % (blk, blk+2,  path,(phash + blk) % len(dataserv),(phash + blk + 2) % len(dataserv))
    else:
      if ack1 == True:
        print "Corrupted block number: %d of file: %s. Assoicated Server number are %d:" % (blk,  path,(phash + blk) % len(dataserv))

def hash_(path):
    return sum(ord(i) for i in path)

def serverInit():
    portNUM = []
    for i in range(len(sys.argv[2:])):
        portNUM.append(int(sys.argv[2 + i]))
    return portNUM


def rpcInit(port):
    MetaName = "http://localhost:" + str(port)
    return xmlrpclib.ServerProxy(MetaName)

metaserv = rpcInit(sys.argv[2])
corruptNUM = sys.argv[1]
dataserv = []


def main():
    if len(sys.argv) < 4:
        print "Less Arguments supplied. Req. format python corrupt_data.py <No of corrupt server (1/2)> <meta server port> <data server ports> ... </filename>"
        sys.exit(0)
    dataservID = sys.argv[3:-1]
    path = sys.argv[-1]
    for i in range(len(dataservID)):
        dataserv.append(rpcInit(dataservID[i]))
    return corrupt(path)

if __name__ == "__main__":
    main()
