#!/usr/bin/env python

# Group members:
# Suvrat Tedia
# Sidhharth Sharma



"""
Author: David Wolinsky
Version: 0.02

Description:
The XmlRpc API for this library is:
  get(base64 key)
    Returns the value and ttl associated with the given key using a dictionary
      or an empty dictionary if there is no matching key
    Example usage:
      rv = rpc.get(Binary("key"))
      print rv => {"value": Binary, "ttl": 1000}
      print rv["value"].data => "value"
  put(base64 key, base64 value, int ttl)
    Inserts the key / value pair into the hashtable, using the same key will
      over-write existing values
    Example usage:  rpc.put(Binary("key"), Binary("value"), 1000)
  print_content()
    Print the contents of the HT
  read_file(string filename)
    Store the contents of the Hahelperable into a file
  write_file(string filename)
    Load the contents of the file into the Hahelperable
"""

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest, hashlib, os.path, shelve, errno
from datetime import datetime, timedelta
from xmlrpclib import Binary
from socket import error as socket_error


# Presents a HT interface
class SimpleHT:
    def __init__(self):
        self.data = {}
        self.replica = {}
        self.nxtSERVER = 0
        self.prvSERVER = 0
        self.dataserv = self.serverInit()
        self.serverID = int(sys.argv[1])
        self.replicaSETUP()
        self.f = "recover_data" + str(self.serverID) + ".txt"
        # On Crash check if the recovery exists
        if (os.path.exists(self.f) == True):
            self.data_replica_restore(self.f)
        else:
            try:
                self.data_replica_recover(self.f)
            except socket_error as serr:
                if serr.errno != errno.ECONNREFUSED:
                    raise serr

    def count(self):
        return len(self.data)

    # Retrieve something from the HT
    def get(self, key):
        # Default return value
        rv = ""
        # FS = {}
        # If the key is in the data structure, return properly formated results
        key = key.data
        if key == "recover":
            FS = self.data
            return Binary(pickle.dumps(FS))
        else:
            if key in self.data:
                rv = Binary(self.data[key]["data"])
                chksumDATA = self.data[key]["checksum"]
                try:
                    replica = pickle.loads(self.rpcInit(self.nxtSERVER).rpc_read_replica(Binary(key)).data)
                    replica_data = replica["data"]
                    chksumREPLICA = replica["checksum"]
                except socket_error as serr:
                    if serr.errno != errno.ECONNREFUSED:
                        raise serr
                    replica_data = self.data[key]["data"]
                    chksumREPLICA = self.data[key]["checksum"]
                if (self.get_checksum(self.data[key]["data"]) == chksumDATA):
                    if (self.get_checksum(self.data[key]["data"]) == self.get_checksum(replica_data)):
                        if (chksumDATA != chksumREPLICA):
                            self.rpcInit(self.nxtSERVER).write_file_replica(Binary(key), rv)
                    else:
                        self.rpcInit(self.nxtSERVER).write_file_replica(Binary(key), rv)
                else:
                    i = 0
                    temp = ""
                    while(key[i] != '/'):
                        temp += key[i]
                        i += 1
                    print "corrected Block Number: %s of file: %s" % (temp,key[i+1:])
                    rv = pickle.loads(self.rpcInit(self.nxtSERVER).rpc_read_replica(Binary(key)).data)
                    self.put(Binary(key), Binary(rv["data"]))
                    rv = Binary(rv["data"])
                return rv

    # Insert something into the HT
    def put(self, key, value):
        # Remove expired entries
	print "putting in self.data"
	print "key is %s" %(key.data)
	print "value is %s" %(value.data)
        self.data[key.data] = dict(data=value.data, checksum=self.get_checksum(value.data))
        f = "recover_data" + str(self.serverID) + ".txt"
        d = shelve.open(f)
        d[key.data] = dict(D=value.data, R="NULL")
        d.close()
        return True

    def read_file_replica(self, filename):
        # print 'getting', key
        # Default return value
        rv = ""
        # If the key is in the data structure, return properly formated results
        filename = filename.data
        if filename == "recover":
            FS = self.replica
            return Binary(pickle.dumps(FS))
        else:
            if filename in self.replica:
                rv = Binary(self.replica[filename]["data"])
            return rv

    def write_file_replica(self, filename, value):
        # print 'putting', key, value
        # Remove expired entries
	print "putting in self.replica"
	print "key is %s" %(filename.data)
	print "value is %s" %(value.data)
        self.replica[filename.data] = dict(data=value.data, checksum=self.get_checksum(value.data))
        f = "recover_data" + str(self.serverID) + ".txt"
        d = shelve.open(f)
        d[filename.data] = dict(D="NULL", R=value.data)
        d.close()
        return True

    # Load contents from a file
    def read_file(self, filename):
        f = open(filename.data, "rb")
        self.data = pickle.load(f)
        f.close()
        return True

    # Clear all contents
    def clear(self):
        self.data.clear()
        self.replica.clear()
	d = shelve.open(self.f)
	d.clear()
	d.close()
        return True

    # Delete a file
    def remove(self, key):
        # print 'removing', key
        if key.data in self.data:
            del self.data[key.data]
            d = shelve.open(self.f)
            print "deleting the file with key %s" % (key)
            del d[key.data]
            d.close()
            return True
        else:
            return False

    # Delete a file
    def remove_replica(self, key):
        # print 'removing', key
        if key.data in self.replica:
            del self.replica[key.data]
            r = shelve.open(self.f)
            print "deleting the file with key %s" % (key)
            del r[key.data]
            r.close()
            return True
        else:
            return False

    # Write contents to a file
    def write_file(self, filename):
        f = open(filename.data, "wb")
        pickle.dump(self.data, f)
        f.close()
        return True

    # Print the contents of the hashtable
    def print_content(self):
        print self.data
        return True

    def data_replica_restore(self, data):
        d_r = shelve.open(data)
        d_r_list = list(d_r.keys())
        for key in d_r_list:
            if d_r[key]["R"] == "NULL":
                self.data[key] = dict(data=d_r[key]["D"], checksum=self.get_checksum(d_r[key]["D"]))
            else:
                self.replica[key] = dict(data=d_r[key]["R"], checksum=self.get_checksum(d_r[key]["R"]))
        d_r.close()

    def data_replica_recover(self, data):
        print "Restoringggggggg"
        self.replica = pickle.loads(self.rpcInit(self.prvSERVER).get(Binary("recover")).data)
        # pickle.loads(self.metaserv.get(Binary(path)).data)
        self.data = pickle.loads(self.rpcInit(self.nxtSERVER).read_file_replica(Binary("recover")).data)
        print "Checkpoint"
        d_r = shelve.open(data)

        for key in self.replica.keys():
            d_r[key] = dict(D="NULL", R=self.replica[key]["data"])
        for key in self.data.keys():
            d_r[key] = dict(D=self.data[key]["data"], R="NULL")
        d_r.close()

    def replicaSETUP(self):
        numSERVERS = len(self.dataserv)
        if self.serverID == 0 or self.serverID == (numSERVERS - 1):
            if self.serverID == 0:
                self.nxtSERVER = self.dataserv[self.serverID + 1]
                self.prvSERVER = self.dataserv[numSERVERS - 1]
            else:
                self.nxtSERVER = self.dataserv[0]
                self.prvSERVER = self.dataserv[self.serverID - 1]
        else:
            self.nxtSERVER = self.dataserv[self.serverID + 1]
            self.prvSERVER = self.dataserv[self.serverID - 1]

    def serverInit(self):
        portNUM = []
        for i in range(len(sys.argv[2:])):
            portNUM.append(int(sys.argv[2 + i]))
        return portNUM

    def rpcInit(self, port):
        MetaName = "http://localhost:" + str(int(port))
        return xmlrpclib.ServerProxy(MetaName)

    def get_checksum(self, string):
        temp = hashlib.sha256(string.encode('utf-8')).hexdigest()
        return str(temp)

    def rpc_read_replica(self, filename):
        print "Inside rpc_read_replica"
        rv = self.replica[filename.data]
        return Binary(pickle.dumps(rv))

    def rpc_read_data(self, filename):
        rv = self.data[filename.data]
        return Binary(pickle.dumps(rv))

    def pingserv(self):
        return True

    def corrupt_data(self, filename):
        print
        if filename.data not in self.data.keys():
            return False
        else:
            size = len(self.data[filename.data]["data"])
            corruptD = ['X' for i in range(size)]
            corruptD = ''.join(corruptD)
            self.data[filename.data]["data"] = corruptD
            print self.get_checksum(self.data[filename.data]["data"]), self.data[filename.data]["checksum"]
            return True


def main():
    portID = int(sys.argv[1])
    portNUM = []
    print sys.argv[2:]
    for i in range(len(sys.argv[2:])):
        portNUM.append(int(sys.argv[2 + i]))
    return serve(portNUM[portID])


# Start the xmlrpc server
def serve(port):
    file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
    file_server.register_introspection_functions()
    sht = SimpleHT()
    file_server.register_function(sht.clear)
    file_server.register_function(sht.get)
    file_server.register_function(sht.put)
    file_server.register_function(sht.print_content)
    # file_server.register_function(sht.read_file_data)
    file_server.register_function(sht.read_file_replica)
    # file_server.register_function(sht.write_file_data)
    file_server.register_function(sht.write_file_replica)
    file_server.register_function(sht.remove)
    file_server.register_function(sht.remove_replica)
    file_server.register_function(sht.pingserv)
    #file_server.register_function(sht.checksum)
    file_server.register_function(sht.rpc_read_replica)
    file_server.register_function(sht.rpc_read_data)
    file_server.register_function(sht.get_checksum)
    file_server.register_function(sht.corrupt_data)
    file_server.serve_forever()


# Execute the xmlrpc in a thread ... needed for testing
class serve_thread:
    def __call__(self, port):
        serve(port)


# Wrapper functions so the tests don't need to be concerned about Binary blobs
class Helper:
    def __init__(self, caller):
        self.caller = caller

    def put(self, key, val, ttl):
        return self.caller.put(Binary(key), Binary(val), ttl)

    def get(self, key):
        return self.caller.get(Binary(key))

    def write_file(self, filename):
        return self.caller.write_file(Binary(filename))

    def read_file(self, filename):
        return self.caller.read_file(Binary(filename))


class SimpleHTTest(unittest.TestCase):
    def test_direct(self):
        helper = Helper(SimpleHT())
        self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
        self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
        self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
        self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
        self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
        self.assertTrue(helper.put("test", "test1", 2), "Failed to put")
        self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
        time.sleep(2)
        self.assertEqual(helper.get("test"), {}, "Failed expire")
        self.assertTrue(helper.put("test", "test2", 20000))
        self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

        helper.write_file("test")
        helper = Helper(SimpleHT())

        self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
        helper.read_file("test")
        self.assertEqual(helper.get("test")["value"], "test2", "Load unsuccessful!")
        self.assertTrue(helper.put("some_other_key", "some_value", 10000))
        self.assertEqual(helper.get("some_other_key")["value"], "some_value", "Different keys")
        self.assertEqual(helper.get("test")["value"], "test2", "Verify contents")

    # Test via RPC
    def test_xmlrpc(self):
        output_thread = threading.Thread(target=serve_thread(), args=(51234,))
        output_thread.setDaemon(True)
        output_thread.start()

        time.sleep(1)
        helper = Helper(xmlrpclib.Server("http://127.0.0.1:51234"))
        self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
        self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
        self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
        self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
        self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
        self.assertTrue(helper.put("test", "test1", 2), "Failed to put")
        self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
        time.sleep(2)
        self.assertEqual(helper.get("test"), {}, "Failed expire")
        self.assertTrue(helper.put("test", "test2", 20000))
        self.assertEqual(helper.get("test")["value"], "test2", "Store new value")


if __name__ == "__main__":
    main()
