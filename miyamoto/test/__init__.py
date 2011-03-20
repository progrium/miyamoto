import socket

import gevent
import gevent.event
import gevent.pywsgi

from miyamoto import start

def _unused_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    addr, port = s.getsockname()
    s.close()
    return port

class Cluster(object):
    def __init__(self, size=2, replica_factor=2):
        self.size = size
        self.replica_factor = replica_factor
        self.nodes = []
        self.leader = '127.0.0.1'
    
    def start(self, wait=True):
        for n in xrange(self.size):
            self.add(False)
        if wait:
            for node in self.nodes:
                node.ready.wait()
    
    def stop(self):
        for node in self.nodes:
            node.terminate()
        self.nodes = []
    
    def add(self, wait=True):
        interface = '127.0.0.%s' % (len(self.nodes)+1)
        node = start(self.leader, self.replica_factor, interface)
        self.nodes.append(node)
        if wait:
            node.ready.wait()
        return node
        
    def __len__(self):
        return len(self.nodes)
    
class TaskCountdown(object):
    def __init__(self, count=1):
        self.count = count
        self.server = gevent.pywsgi.WSGIServer(('', _unused_port()), self._app, log=None)
        self.done = gevent.event.Event()
    
    def start(self, count=None):
        if count:
            self.count = count
        self.server.start()
        self.url = 'http://%s:%s' % (self.server.server_host, self.server.server_port)
        return self.url
    
    def wait(self, timeout=None):
        self.done.wait(timeout)
    
    def finished(self):
        return self.done.isSet()
    
    def _app(self, env, start_response):
        self.count -= 1
        if self.count <= 0:
            self.done.set()
            self.server.kill()
        start_response("200 OK", {})
        return ['ok']
