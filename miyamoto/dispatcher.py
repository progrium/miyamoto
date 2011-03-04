import socket
import urllib2
try:
    import json
except ImportError:
    import simplejson as json

import gevent
import gevent.monkey
import gevent.server
import gevent.socket
import gevent.queue
import urllib3

gevent.monkey.patch_socket()

from task import Task

class Dispatcher(object):
    def __init__(self, workers=10):
        # hardcoding for now
        self.workers = workers
        self.server = gevent.server.StreamServer(('127.0.0.1', 6002), self._connection_handler)
        self.queue = gevent.queue.Queue()
        self.connections = {}
    
    def start(self, block=True):
        self.server.start()
        for n in range(self.workers):
            gevent.spawn(self._dispatcher)
        
        while block:
            gevent.sleep(1)
    
    def _dispatcher(self):
        while True:
            task = Task.unserialize(self.queue.get())
            host = urllib3.get_host(task.url)
            if not host in self.connections:
                self.connections[host] = urllib3.connection_from_url(task.url)
            self.connections[host].urlopen(url=task.url, method=task.method) # that's it for now
    
    def _connection_handler(self, socket, address):
        print "new connection"
        fileobj = socket.makefile()
        while True:
            try:
                line = fileobj.readline().strip()
            except IOError:
                line = None
            if line:
                self.queue.put(line)
            else:
                break
        print "conn drop"
    
