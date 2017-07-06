#!/usr/bin/env python

# Group members:
# Suvrat Tedia
# Sidhharth Sharma

import logging
import pickle
import random

from collections import defaultdict
from errno import ENOENT, ENOTEMPTY, ECONNREFUSED
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from xmlrpclib import Binary, ServerProxy
from copy import deepcopy
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from socket import error as socket_error
from time import sleep

block_size = 8

if not hasattr(__builtins__, 'bytes'):
    bytes = str


class Memory(LoggingMixIn, Operations):
    """Implements a hierarchical file system by using FUSE virtual filesystem.
       The file structure and data are stored in local memory in variable.
       Data is lost when the filesystem is unmounted"""

    def __init__(self, mport, dports):
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
	#Proxy connections for metaserver and dataserver
        self.metaserv = ServerProxy("http://localhost:" + str(int(mport)))
        self.dataserv = [ServerProxy("http://localhost:" + str(int(i))) for i in dports]
        self.metaserv.clear()
	#Clear all the dataservers when the client starts
        for i in self.dataserv:
            i.clear()
        now = time()
	# Put root dictionary into the metaserver
        root_dict = dict(name='/', IsFile=False, st_mode=(S_IFDIR | 0o755), st_ctime=now,
                         st_mtime=now, st_atime=now, st_nlink=2)
        self.put('/', root_dict)

        # The key 'files' holds a dict of filenames(and their attributes
        #  and 'files' if it is a directory) under each level

	#Funtion for getting attributes from the metaserver
    def get(self, path):
        return pickle.loads(self.metaserv.get(Binary(path)).data)
	
	#Function for putting file attributes into the metaserver
    def put(self, path, meta):
        return self.metaserv.put(Binary(path), Binary(pickle.dumps(meta)))

	#Function for getting data from the dataserver
	#If server is down get the data from the adjacent dataserver
    def getdata(self, path, blks):
        rv = []
        phash = self.hash_(path)
        for i in range(len(blks)):
            try:
		print "phash is %d" %(phash)
		print "################### Server Number: %d ############### " % ((phash + blks[i]) % len(self.dataserv))
		print "key is %s" %(str(blks[i]) + path)
                rv.append(self.dataserv[(phash + blks[i]) % len(self.dataserv)].get(Binary(str(blks[i]) + path)).data)
            except socket_error as serr:
                if serr.errno != ECONNREFUSED:
                    raise serr
                if (phash + blks[i]) % len(self.dataserv) == len(self.dataserv)-1:
                    rv.append(self.dataserv[0].read_file_replica(Binary(str(blks[i]) + path)).data)
                else:
                    print self.dataserv[(phash + blks[i]) % len(self.dataserv) + 1].read_file_replica(Binary(str(blks[i]) + path)).data
                    rv.append(self.dataserv[(phash + blks[i]) % len(self.dataserv) + 1].read_file_replica(Binary(str(blks[i]) + path)).data)
        return rv

	#Function to put data into the dataserver and replica-data into replica server
	#If any server in which the data has to be written is down,this function blocks the write calll until the server is up again
    def putdata(self, path, blks, datablks):
        phash = self.hash_(path)
        for i in range(len(blks)):
            print "inside for"
            ack1 = False
	    ack2 = False
	    print "iteration is %d" %(i)
	    print "phash is %d" %(phash)
            print "################### Server Number: %d ############### " % ((phash + blks[i]) % len(self.dataserv))
	    print "key is %s" %(str(blks[i]) + path)
            print "data is %s" %(datablks[i])
            while (ack1 == False):
                try:
		    ack1 = self.dataserv[(phash + blks[i]) % len(self.dataserv)].put(Binary(str(blks[i]) + path),
                                                                            Binary(datablks[i]))
		except socket_error as serr:
                    if serr.errno != ECONNREFUSED:
                    	raise serr
		    pass
	    while (ack2 == False):
		try:
		    if (phash + blks[i]) % len(self.dataserv) == len(self.dataserv) - 1:
                        ack2 = self.dataserv[0].write_file_replica(Binary(str(blks[i]) + path), Binary(datablks[i]))
                    else:
                        ack2 = self.dataserv[(phash + blks[i]) % len(self.dataserv) + 1].write_file_replica(Binary(str(blks[i]) + path), Binary(datablks[i]))
                except socket_error as serr:
                    if serr.errno != ECONNREFUSED:
                    	raise serr
                    pass
        return (ack1 and ack2)


	#Function to remove the blocks from the datasever
    def purgedata(self, path, blks):
        phash = self.hash_(path)
        print "################removing####################"
        for i in blks:
	   ack1 = False
	   ack2 = False
	   while (ack1 == False):
	        try:
            	   ack1 = self.dataserv[(phash + blks[i]) % len(self.dataserv)].remove(Binary(str(blks[i]) + path))
	        except socket_error as serr:
                   if serr.errno != ECONNREFUSED:
                      raise serr
		   pass
           while (ack2 == False):
	        try:
            	   if (phash + blks[i]) % len(self.dataserv) == len(self.dataserv) - 1:
                       ack2 = self.dataserv[0].remove_replica(Binary(str(blks[i]) + path))
            	   else:
                       ack2 = self.dataserv[(phash + blks[i]) % len(self.dataserv) + 1].remove_replica(Binary(str(blks[i]) + path))
	        except socket_error as serr:
                      if serr.errno != ECONNREFUSED:
                         raise serr
		      pass

	#Function for computing hash for any given path
    def hash_(self, path):
        return sum(ord(i) for i in path)

    def chmod(self, path, mode):
        parent = path[:path.rfind('/')]  # key for parent
        if len(parent) == 0:
            parent = '/'
        temp_dir = self.get(parent)
        temp_dir[path]['st_mode'] &= 0o770000
        temp_dir[path]['st_mode'] |= mode
        self.put(parent, temp_dir)
        if temp_dir[path]['IsFile'] == False:
            temp_dir1 = self.get(path, host_name)
            temp_dir1['st_mode'] &= 0o770000
            temp_dir1['st_mode'] |= mode
            self.put(path, temp_dir1)
        return 0

    def chown(self, path, uid, gid):
        parent = path[:path.rfind('/')]  # key for parent
        if len(parent) == 0:
            parent = '/'
        temp_dir = self.get(parent)
        temp_dir[path]['st_uid'] = uid
        temp_dir[path]['st_gid'] = gid
        self.put(parent, temp_dir)
        if temp_dir[path]['IsFile'] == False:
            temp_dir1 = self.get(path, host_name)
            temp_dir1['st_uid'] = uid
            temp_dir1['st_gid'] = gid
            self.put(path, temp_dir1)

    def create(self, path, mode):
        new_file = path[path.rfind('/') + 1:]  # new dir/file
        parent = path[:path.rfind('/')]  # key for parent
        if len(parent) == 0:
            parent = '/'
        temp_dir = self.get(parent)
        temp_dir[path] = dict(name=new_file, IsFile=True, st_mode=(S_IFREG | mode), st_nlink=1,
                              st_size=0, st_ctime=time(), st_mtime=time(),
                              st_atime=time())
        self.put(parent, temp_dir)
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        parent = path[:path.rfind('/')]  # key for parent
        if len(parent) == 0:
            parent = '/'
        if path == '/':
            return self.get(path)
        else:
            temp_dir = self.get(parent)
            if path not in temp_dir:
                raise FuseOSError(ENOENT)
            else:
                if temp_dir[path]['IsFile'] == True:
                    return temp_dir[path]
                else:
                    return self.get(path)

    def getxattr(self, path, name, position=0):
        parent = path[:path.rfind('/')]  # key for parent
        if len(parent) == 0:
            parent = '/'
        if path == '/':
            temp_dir = self.get(path)
            attrs = temp_dir.get('attrs', {})
        else:
            temp_dir = self.get(parent)
            attrs = temp_dir[path].get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''  # Should return ENOATTR

    def listxattr(self, path):
        p = self.traverse(path)
        attrs = p.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        new_dir = path[path.rfind('/') + 1:]  # new dir
        parent = path[:path.rfind('/')]  # key for parent
        if len(parent) == 0:
            parent = '/'
        dic_meta_data = dict(name=new_dir, IsFile=False, st_mode=(S_IFDIR | mode), st_nlink=2,
                             st_ctime=time(), st_mtime=time(),
                             st_atime=time())
        self.put(path, dic_meta_data)
        temp_dir = self.get(parent)
        temp_dir['st_nlink'] += 1
        temp_dir[path] = dict(name=new_dir, IsFile=False, st_mode=(S_IFDIR | 0o755), st_ctime=time(),
                              st_mtime=time(), st_atime=time(), st_nlink=2)
        self.put(parent, temp_dir)

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        parent = path[:path.rfind('/')]  # key for parent
        if len(parent) == 0:
            parent = '/'
        p = self.get(parent)
        if offset + size > p[path]['st_size']:
            size = p[path]['st_size'] - offset
        dd = ''.join(self.getdata(path, range(offset // block_size, (offset + size - 1) // block_size + 1)))
        dd = dd[offset % block_size:offset % block_size + size]
        return dd

    def readdir(self, path, fh):
        temp_dir = self.get(path)
        return ['.', '..'] + [temp_dir[x]['name'] for x in temp_dir if
                              x not in ['st_mode', 'st_ctime', 'st_mtime', 'st_atime', 'st_nlink', 'IsFile', 'name']]

    def readlink(self, path):
        parent = path[:path.rfind('/')]  # key for parent
        if len(parent) == 0:
            parent = '/'
        p = self.get(parent)
        return ''.join(self.getdata(path, range((p[path]['st_size'] // block_size) + 1)))

    def removexattr(self, path, name):
        p = self.traverse(path)
        attrs = p.get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass  # Should return ENOATTR

    def rename(self, old, new):
        old_parent = old[:old.rfind('/')]  # key for parent
        if len(old_parent) == 0:
            old_parent = '/'
        new_parent = new[:new.rfind('/')]  # key for parent
        if len(new_parent) == 0:
            new_parent = '/'
	old_parent_dir = self.get(old_parent)
	new_parent_dir = self.get(new_parent)
	file_size = old_parent_dir[old]["st_size"]
	total_blocks = (file_size-1)//block_size + 1;
        
	if old_parent_dir[old]['IsFile'] == True:
	   new_parent_dir[new] = old_parent_dir.pop(old)
           new_parent_dir[new]['name'] = new[new.rfind('/') + 1:]
           if new_parent == old_parent:
              new_parent_dir.pop(old)
	   self.put(old_parent, old_parent_dir)
           self.put(new_parent, new_parent_dir)
	   dd = self.getdata(old, range(total_blocks))
	   self.purgedata(old,range(total_blocks))
	   self.putdata(new,range(total_blocks),dd)
	
	
  

    def rmdir(self, path):
        parent = path[:path.rfind('/')]  # key for parent
        if len(parent) == 0:
            parent = '/'
        temp_dir = self.get(path)
        if len(temp_dir) > 7:
            raise FuseOSError(ENOTEMPTY)
        else:
            temp_dir = {}
        self.put(path, temp_dir)
        temp_dir1 = self.get(parent)
        temp_dir1.pop(path)
        temp_dir1['st_nlink'] -= 1
        self.put(parent, temp_dir1)

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        p = self.traverse(path)
        attrs = p.setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        new_name = target[target.rfind('/') + 1:]  # new dir
        parent = target[:target.rfind('/')]
        if len(parent) == 0:
            parent = '/'
        temp_dir = self.get(parent)
        temp_dir[target] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                st_size=len(source), name=new_name, IsFile=True)
        self.put(parent, temp_dir)
        blocks = [source[i:i + block_size] for i in range(0, len(source), block_size)]
        self.putdata(target, range(len(blocks)), blocks)

    def truncate(self, path, length, fh=None):
        parent = path[:path.rfind('/')]  # key for parent
        if len(parent) == 0:
            parent = '/'
        p = self.get(parent)
        print length
        currblks = range((p[path]['st_size'] - 1) // block_size + 1)
        newblks = range((length - 1) // block_size + 1)
        print currblks
        print newblks
        print newblks[:-1]
        # create new blocks as needed
        blks_to_create = list(set(newblks) - set(currblks))
        print blks_to_create
        if blks_to_create != []:
            self.putdata(path, blks_to_create, ['\x00' * block_size] * len(blks_to_create))
        # purge existing blocks as required
        blks_to_purge = list(set(currblks) - set(newblks))
        print blks_to_purge
        self.purgedata(path, blks_to_purge)
        # last block trunc
        if len(newblks) > 0:
            if newblks[-1] in currblks:
                self.putdata(path, [newblks[-1]], [self.getdata(path, [newblks[-1]])[0][:length % block_size]])
            else:
                self.putdata(path, [newblks[-1]], ['\x00' * (length % block_size)])
        p[path]['st_size'] = length
        self.put(parent, p)

    def unlink(self, path):
        parent = path[:path.rfind('/')]  # key for parent
        if len(parent) == 0:
            parent = '/'
        temp_dir = self.get(parent)
        file_size = temp_dir[path]["st_size"]
        total_blocks = file_size // block_size + 1
        blks_to_purge = [x for x in range(total_blocks)]
        self.purgedata(path,blks_to_purge)
        temp_dir.pop(path)
        self.put(parent, temp_dir)


    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        parent = path[:path.rfind('/')]  # key for parent
        if len(parent) == 0:
            parent = '/'
        temp_dir = self.get(parent)
        temp_dir[path]['st_atime'] = atime
        temp_dir[path]['st_mtime'] = mtime
        self.put(parent, temp_dir)

    def write(self, path, data, offset, fh):
        parent = path[:path.rfind('/')]
        if len(parent) == 0:
            parent = '/'
        p = self.get(parent)
        currblks = range((p[path]['st_size'] - 1) // block_size + 1)
        if offset > p[path]['st_size']:
            lfill = [(self.getdata(path, [i])[0] if i in currblks else '').ljust(block_size, '\x00') for i in
                     range(offset // block_size)] \
                    + [(self.getdata(path, [offset // block_size])[0][
                        :offset % block_size] if offset // block_size in currblks else '').ljust(offset % block_size,
                                                                                                 '\x00')]
            self.putdata(path, range(0, offset // block_size), lfill)
        size = len(data)
        sdata = [data[:block_size - (offset % block_size)]] + [data[i:i + block_size] for i in
                                                               range(block_size - (offset % block_size), size,
                                                                     block_size)]
        blks = range(offset // block_size, (offset + size - 1) // block_size + 1)
        mod = blks[:]
        mod[0] = (self.getdata(path, [blks[0]])[0][:offset % block_size] if blks[0] in currblks else '').ljust(
            offset % block_size, '\x00') + sdata[0]
        if len(mod[0]) != block_size and blks[0] in currblks:
            mod[0] = mod[0] + self.getdata(path, [blks[0]])[0][len(mod[0]):]
        mod[1:-1] = sdata[1:-1]
        if len(blks) > 1:
            mod[-1] = sdata[-1] + (self.getdata(path, [blks[-1]])[0][len(sdata[-1]):] if blks[-1] in currblks else '')
        self.putdata(path, blks, mod)
        p[path]['st_size'] = offset + size if offset + size > p[path]['st_size'] else p[path]['st_size']
        self.put(parent, p)
        return size


if __name__ == '__main__':
    if len(argv) < 4:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(argv[2], argv[3:]), argv[1], foreground=True, debug=True)
