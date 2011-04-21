import gevent.monkey; gevent.monkey.patch_all()

import socket
import urllib2
try:
    import json
except ImportError:
    import simplejson as json

import gevent
import gevent.pywsgi
import gevent.queue

from cluster import ClusterManager
from scheduler import DistributedScheduler
from task import Task
import constants

class QueueServer(object):
    # TODO: make args list
    def __init__(self, leader, replica_factor=constants.DEFAULT_REPLICA_FACTOR, 
        replica_offset=constants.DEFAULT_REPLICA_SECS_OFFSET, interface=None, 
        frontend_port=constants.DEFAULT_FRONTEND_PORT, 
        backend_port=constants.DEFAULT_BACKEND_PORT, 
        cluster_port=constants.DEFAULT_CLUSTER_PORT):
        if interface is None:
            interface = socket.gethostbyname(socket.gethostname())
        self.queue = gevent.queue.Queue()
        self.frontend = gevent.pywsgi.WSGIServer((interface, frontend_port), self._frontend_app, log=None)
        self.scheduler = DistributedScheduler(self.queue, leader, replica_factor=replica_factor, 
            replica_offset=replica_offset, interface=interface, port=backend_port, cluster_port=cluster_port)
        self._ready_event = None
    
    def start(self, block=True):
        self.frontend.start()
        self.scheduler.start()
        
        while not self.frontend.started:
            gevent.sleep(1)
        
        self._ready_event.set()
        
        while block:
            gevent.sleep(1)
    
    def _frontend_app(self, env, start_response):
        try:
            queue_name = env['PATH_INFO']
            content_type = env['CONTENT_TYPE']
            body = env['wsgi.input'].read()
            task = Task(queue_name, content_type, body)
            
            self.scheduler.schedule(task) # TODO: needs queue on the other end
            
            start_response('200 OK', [('Content-Type', 'application/json')])
            return ['{"status": "scheduled", "id": "%s"}\n' % task.id]
        except NotImplemented, e:
            start_response('500 Error', [('Content-Type', 'application/json')])
            return [json.dumps({"status": "error", "reason": repr(e)})]
