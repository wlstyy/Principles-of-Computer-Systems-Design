"""SecureXMLRPCServer.py - simple XML RPC server supporting SSL.
 
Based on articles:
    1. http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/81549
    2. http://code.activestate.com/recipes/496786-simple-xml-rpc-server-over-https/
    3. http://stackoverflow.com/questions/5690733/xmlrpc-server-over-https-in-python-3
"""
 
# Configure below
LISTEN_HOST='localhost'     # You should not use '' here, unless you have a real FQDN.
LISTEN_PORT=8443
 
KEYFILE='privkey.pem'           # Replace with your PEM formatted key file
CERTFILE='cacert.pem'  # Replace with your PEM formatted certificate file
# Configure above
 
import SocketServer
import BaseHTTPServer
import SimpleHTTPServer
import SimpleXMLRPCServer
 
import socket, ssl
import sys, getopt, pickle, time, threading, xmlrpclib, unittest
import random
from datetime import datetime, timedelta
 
class SecureXMLRPCServer(BaseHTTPServer.HTTPServer,SimpleXMLRPCServer.SimpleXMLRPCDispatcher):
    def __init__(self, server_address, HandlerClass, logRequests=True):
        """Secure XML-RPC server.
        It it very similar to SimpleXMLRPCServer but it uses HTTPS for transporting XML data.
        """
        self.logRequests = logRequests
 
        try:
            SimpleXMLRPCServer.SimpleXMLRPCDispatcher.__init__(self)
        except TypeError:
            # An exception is raised in Python 2.5 as the prototype of the __init__
            # method has changed and now has 3 arguments (self, allow_none, encoding)
            #
            SimpleXMLRPCServer.SimpleXMLRPCDispatcher.__init__(self, False, None)
 
        SocketServer.BaseServer.__init__(self, server_address, HandlerClass)
 
        self.socket = ssl.wrap_socket(socket.socket(), server_side=True, certfile=CERTFILE,
                            keyfile=KEYFILE, ssl_version=ssl.PROTOCOL_SSLv23)
 
        self.server_bind()
        self.server_activate()
 
class SecureXMLRpcRequestHandler(SimpleXMLRPCServer.SimpleXMLRPCRequestHandler):
    """Secure XML-RPC request handler class.
    It it very similar to SimpleXMLRPCRequestHandler but it uses HTTPS for transporting XML data.
    """
 
    def setup(self):
        self.connection = self.request
        self.rfile = socket._fileobject(self.request, "rb", self.rbufsize)
        self.wfile = socket._fileobject(self.request, "wb", self.wbufsize)
 
    def do_POST(self):
        """Handles the HTTPS POST request.
 
        It was copied out from SimpleXMLRPCServer.py and modified to shutdown the socket cleanly.
        """
 
        try:
            # get arguments
            data = self.rfile.read(int(self.headers["content-length"]))
            # In previous versions of SimpleXMLRPCServer, _dispatch
            # could be overridden in this class, instead of in
            # SimpleXMLRPCDispatcher. To maintain backwards compatibility,
            # check to see if a subclass implements _dispatch and dispatch
            # using that method if present.
            response = self.server._marshaled_dispatch(
                    data, getattr(self, '_dispatch', None)
                )
        except: # This should only happen if the module is buggy
            # internal error, report as HTTP server error
            self.send_response(500)
            self.end_headers()
        else:
            # got a valid XML RPC response
            self.send_response(200)
            self.send_header("Content-type", "text/xml")
            self.send_header("Content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)
 
            # shut down the connection
            self.wfile.flush()
 
            #modified as of http://docs.python.org/library/ssl.html
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
 
class Master:
    def __init__(self):
        self.data={}
        self.servers={}
        self.opnum=0
        self.next_check = datetime.now() + timedelta(minutes = 5)
        random.seed()
    def count(self):
        # Remove expired entries
        self.next_check = datetime.now() - timedelta(minutes = 5)
        self.check()
        return len(self.data)
    
    def get(self, key):
        #prob = random.random()
        #if prob < 0.1:
        #  raise StandardError

        # Remove expired entries
        self.check()
        # Default return value
        rv = {}
        # If the key is in the data structure, return properly formated results
        if key in self.data:
            ent = self.data[key]
            now = datetime.now()
            if ent[1] > now:
                ttl = (ent[1] - now).seconds
                rv = {"value": ent[0], "ttl": ttl}
            else:
                del self.data[key]
        return rv

    def getServer(self):
        server=''
        minsize=float('inf')
        for k, v in self.servers.iteritems():
            #print k
            if v<minsize:
                server=k
                minsize=v
        #print 'server address is'+server
        return server

    def put(self, key, value, ttl):
        # Remove expired entries
        self.check()
        end = datetime.now() + timedelta(seconds = ttl)
        self.data[key] = (value, end)
        return True


    def read_file(self, filename):
        f = open(filename, "rb")
        self.data = pickle.load(f)
        f.close()
        return True

    # Write contents to a file
    def write_file(self, filename):
        f = open(filename, "wb")
        pickle.dump(self.data, f)
        f.close()
        return True

    def register(self, url):
        if url not in self.data:
            print url
            self.servers[url]=0
        return True

    def changeSize(self, key, server, newsize):
        si=newsize
        if key in self.data:
            orgvalue=pickle.loads(self.data[key][0])['st_size']
            si=si-orgvalue
        ssize=self.servers[server]
        ssize=ssize+si
        self.servers[server]=ssize
        return True
   
    def check(self):
    	    now = datetime.now()
    	    if self.next_check > now:
    	    	    return
    	    self.next_check = datetime.now() + timedelta(minutes = 5)
    	    to_remove = []
    	    for key, value in self.data.items():
    	    	    if value[1] < now:
    	    	    	    to_remove.append(key)
    	    for key in to_remove:
    	    	    value=pickle.loads(self.data[key][0])
    	    	    server=value['contents']
    	    	    size=value['st_size']
    	    	    orgsize=self.servers[server]
    	    	    self.servers[server]=orgsize-size
    	    	    del self.data[key]

def test(HandlerClass = SecureXMLRpcRequestHandler, ServerClass = SecureXMLRPCServer):
    server_address = (LISTEN_HOST, LISTEN_PORT)
    server = ServerClass(server_address, HandlerClass)
    sht=Master()
    server.register_introspection_functions()
    server.register_function(sht.get)
    server.register_function(sht.put)
    server.register_function(sht.getServer)
    server.register_function(sht.read_file)
    server.register_function(sht.write_file)
    server.register_function(sht.register)
    server.register_function(sht.changeSize)
 
    print "Serving HTTPS on %s, port %d" % (LISTEN_HOST, LISTEN_PORT)
    server.serve_forever()
 
if __name__ == '__main__':
    test()