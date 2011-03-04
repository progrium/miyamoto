import socket
import urllib2

import gevent
import gevent.monkey
import gevent.pywsgi
import gevent.queue

gevent.monkey.patch_all(thread=True)

from cluster import ClusterManager
from scheduler import DistributedScheduler
from task import Task


class QueueServer(object):
    def __init__(self, leader, replica_factor=2, replica_offset=5, interface=None, 
        frontend_port=8088, backend_port=6001, cluster_port=6000):
        if interface is None:
            interface = socket.gethostbyname(socket.gethostname())
        self.queue = gevent.queue.Queue()
        self.frontend = gevent.pywsgi.WSGIServer((interface, frontend_port), self._frontend_handler, log=None)
        self.scheduler = DistributedScheduler(self.queue, leader, replica_factor=replica_factor, 
            replica_offset=replica_offset, interface=interface, port=backend_port, cluster_port=cluster_port)
        
    
    def run(self, block=True):
        self.scheduler.start()
        self.frontend.start()
        
        while block:
            gevent.sleep(1)
    
    def _frontend_handler(self, env, start_response):
        try:
            queue_name = env['PATH_INFO']
            content_type = env['CONTENT_TYPE']
            body = env['wsgi.input'].read()
            task = Task(queue_name, content_type, body)
            
            self.scheduler.schedule(task)
            
            start_response('200 OK', [('Content-Type', 'application/json')])
            return ['{"status": "scheduled", "id": "%s"}\n' % task.id]
        except Exception, e:
            start_response('500 Error', [('Content-Type', 'application/json')])
            return ['{"status": "error", "details": "%s"}\n' % repr(e)]
        

if __name__ == '__main__':
    import sys
    
    leader = sys.argv[1]
    interface = sys.argv[2] if len(sys.argv) == 3 else None
    
    print "%s: Using leader %s..." % (interface, leader)

    print "Starting queue server..."
    server = QueueServer(leader, replica_factor=2, interface=interface)
    server.run()
    