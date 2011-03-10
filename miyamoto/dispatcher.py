import gevent.monkey; gevent.monkey.patch_all()

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
import httplib2

from task import Task
import util

class Dispatcher(object):
    def __init__(self, interface, workers=10):
        # hardcoding for now
        self.workers = workers
        self.server = gevent.server.StreamServer((interface, 6002), self._connection_handler)
        self.queue = gevent.queue.Queue()
        self.connections = {}
    
    def start(self, block=True):
        self.server.start()
        for n in xrange(self.workers):
            gevent.spawn(self._dispatcher)
        
        while block:
            gevent.sleep(1)
    
    def _dispatcher(self):
        http = httplib2.Http()
        while True:
            task = Task.unserialize(self.queue.get())
            resp, content = http.request(task.url, method=task.method)
    
    def _connection_handler(self, socket, address):
        print "new connection"
        for line in util.line_protocol(socket):
            self.queue.put(line)
        print "conn drop"
    
