#!/usr/bin/env python
"""
Author: Tao YU
Version: 0.01

A file system that interacts with an xmlrpc HT.
"""

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import sys, pickle, xmlrpclib

class HtProxy:
  """ Wrapper functions so the FS doesn't need to worry about HT primitives."""
  # A hashtable supporting atomic operations, i.e., retrieval and setting
  # must be done in different operations
  def __init__(self):
    	self.rpc=xmlrpclib.ServerProxy('https://localhost:8443')

  # Retrieves a value from the SimpleHT, returns KeyError, like dictionary, if
  # there is no entry in the SimpleHT
  #need to be changed
  def __getitem__(self, key):
    rv = self.get(key)
    if rv == None:
      raise KeyError()
    return rv
    
  # Stores a value in the SimpleHT
  #need to be changed
  def __setitem__(self, key, value):
    self.put(key, value)

  # Sets the TTL for a key in the SimpleHT to 0, effectively deleting it
  def __delitem__(self, key):
    self.put(key, "", 0)
      
  # Retrieves a value from the DHT, if the results is non-null return true,
  # otherwise false
  def __contains__(self, key):
    return self.get(key) != None

#need to be changed
  def get(self, key):
    res = self.rpc.get(key)
    if "value" in res:
      data=res["value"]
      hdata=pickle.loads(data)
      if key!='/':
        fserver=hdata["contents"]
        print fserver
        fsrpc=xmlrpclib.ServerProxy(fserver)
        hdata["contents"]=fsrpc.get(key)["value"]
      return hdata
    else:
      return None

#need to be changed
  def put(self, key, val, ttl=10000):
    if key=='/':
      return self.rpc.put(key, pickle.dumps(val), ttl)
    res = self.rpc.get(key)
    if "value" in res:
      data=pickle.loads(res["value"])
      fserver=data["contents"]
    else:
      fserver=self.rpc.getServer()
    contents=val["contents"]
    val["contents"]=fserver
    print fserver
    fsrpc=xmlrpclib.ServerProxy(fserver)
    if fsrpc.put(key,contents, ttl)==False:
      return 
    self.rpc.changeSize(key, fserver,val['st_size'])
    return self.rpc.put(key, pickle.dumps(val), ttl)

  def read_file(self, filename):
    return self.rpc.read_file(filename)

  def write_file(self, filename):
    return self.rpc.write_file(filename)

class Memory(LoggingMixIn, Operations):
  """Example memory filesystem. Supports only one level of files."""
  def __init__(self, ht):
    self.files = ht
    self.fd = 0
    now = time()
    if '/' not in self.files:
      self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
        st_mtime=now, st_atime=now, st_nlink=2, contents=['/'])

  def chmod(self, path, mode):
    ht = self.files[path]
    ht['st_mode'] &= 077000
    ht['st_mode'] |= mode
    self.files[path] = ht
    return 0

  def chown(self, path, uid, gid):
    ht = self.files[path]
    if uid != -1:
      ht['st_uid'] = uid
    if gid != -1:
      ht['st_gid'] = gid
    self.files[path] = ht
  
  def create(self, path, mode):
    self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1, st_size=0,
        st_ctime=time(), st_mtime=time(), st_atime=time(), contents='')

    ht = self.files['/']
    ht['st_nlink'] += 1
    ht['contents'].append(path)
    self.files['/'] = ht

    self.fd += 1
    return self.fd
  
  def getattr(self, path, fh=None):
    if path not in self.files['/']['contents']:
      raise FuseOSError(ENOENT)
    return self.files[path]
  
  def getxattr(self, path, name, position=0):
    attrs = self.files[path].get('attrs', {})
    try:
      return attrs[name]
    except KeyError:
      return ''    # Should return ENOATTR
  
  def listxattr(self, path):
    return self.files[path].get('attrs', {}).keys()
  
  def mkdir(self, path, mode):
    self.files[path] = dict(st_mode=(S_IFDIR | mode),
        st_nlink=2, st_size=0, st_ctime=time(), st_mtime=time(),
        st_atime=time(), contents=[])
    ht = self.files['/']
    ht['st_nlink'] += 1
    ht['contents'].append(path)
    self.files['/'] = ht

  def open(self, path, flags):
    self.fd += 1
    return self.fd
  
  def read(self, path, size, offset, fh):
    ht = self.files[path]
    if 'contents' in self.files[path]:
      return self.files[path]['contents']
    return None
  
  def readdir(self, path, fh):
    return ['.', '..'] + [x[1:] for x in self.files['/']['contents'] if x != '/']
  
  def readlink(self, path):
    return self.files[path]['contents']
  
  def removexattr(self, path, name):
    ht = self.files[path]
    attrs = ht.get('attrs', {})
    if name in attrs:
      del attrs[name]
      ht['attrs'] = attrs
      self.files[path] = ht
    else:
      pass    # Should return ENOATTR
  
  def rename(self, old, new):
    f = self.files[old]
    self.files[new] = f
    del self.files[old]
    ht = self.files['/']
    ht['contents'].append(new)
    ht['contents'].remove(old)
    self.files['/'] = ht
  
  def rmdir(self, path):
    del self.files[path]
    ht = self.files['/']
    ht['st_nlink'] -= 1
    ht['contents'].remove(path)
    self.files['/'] = ht
  
  def setxattr(self, path, name, value, options, position=0):
    # Ignore options
    ht = self.files[path]
    attrs = ht.get('attrs', {})
    attrs[name] = value
    ht['attrs'] = attrs
    self.files[path] = ht
  
  def statfs(self, path):
    return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
  
  def symlink(self, target, source):
    self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
      st_size=len(source), contents=source)

    ht = self.files['/']
    ht['st_nlink'] += 1
    ht['contents'].append(target)
    self.files['/'] = ht
  
  def truncate(self, path, length, fh=None):
    ht = self.files[path]
    if 'contents' in ht:
      ht['contents'] = ht['contents'][:length]
    ht['st_size'] = length
    self.files[path] = ht
  
  def unlink(self, path):
    ht = self.files['/']
    ht['contents'].remove(path)
    self.files['/'] = ht
    del self.files[path]
  
  def utimens(self, path, times=None):
    now = time()
    ht = self.files[path]
    atime, mtime = times if times else (now, now)
    ht['st_atime'] = atime
    ht['st_mtime'] = mtime
    self.files[path] = ht
  
  def write(self, path, data, offset, fh):
    # Get file data
    ht = self.files[path]
    tmp_data = ht['contents']
    toffset = len(data) + offset
    if len(tmp_data) > toffset:
      # If this is an overwrite in the middle, handle correctly
      ht['contents'] = tmp_data[:offset] + data + tmp_data[toffset:]
    else:
      # This is just an append
      ht['contents'] = tmp_data[:offset] + data
    ht['st_size'] = len(ht['contents'])
    self.files[path] = ht
    return len(data)

if __name__ == "__main__":
  #if len(argv) != 4:
   # print 'usage: %s <mountpoint> <starting port number> <number of servers>' % argv[0]
    #exit(1)
  #url = argv[2]
  # Create a new HtProxy object using the URL specified at the command-line
  fuse = FUSE(Memory(HtProxy()), argv[1], foreground=True)
