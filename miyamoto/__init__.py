import os
import logging
import multiprocessing
import threading
import json
import sys

from gevent_zeromq import zmq
import gevent
import httplib2

from miyamoto.queue import QueueServer
from miyamoto.dispatcher import Dispatcher
from miyamoto import constants

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler(sys.stdout))

class _MiyamotoNode(object):
    def __init__(self, leader, replicas, interface, mode=multiprocessing):
        self.leader = leader
        self.replicas = replicas
        self.interface = interface
        self.ready = multiprocessing.Event()
        self.pool = []
        self.running = False
        
    def start(self):
        if not self.running:
            self._start_dequeuer()
            self._start_enqueuer()
            self.running = True
        
    def join(self):
        for process in self.pool:
            process.join()
    
    def terminate(self, block=True):
        for process in self.pool:
            process.terminate()
            while block and process.is_alive():
                gevent.sleep()
        
    def enqueue(self, queue_name, task):
        url = 'http://%s:%s/%s' % (self.interface, constants.DEFAULT_FRONTEND_PORT, queue_name)
        headers = {'Content-Type': 'application/json'}
        http = httplib2.Http()
        resp, content = http.request(url, method='POST', headers=headers, body=json.dumps(task))
        print ">", resp, content
        if resp['status'] == '200':
            return True
        else:
            return False
    
    def _start_enqueuer(self):
        def enqueuer():
            gevent.sleep(1)
            server = QueueServer(self.leader, replica_factor=self.replicas, interface=self.interface)
            if self.ready:
                server._ready_event = self.ready
            server.start()
        process = multiprocessing.Process(target=enqueuer, name='enqueuer')
        process.start()
        self.pool.append(process)
    
    def _start_dequeuer(self):
        def dequeuer():
            context = zmq.Context()
            dispatcher = Dispatcher(self.interface, context)
            dispatcher.start()
        process = multiprocessing.Process(target=dequeuer, name='dequeuer')
        process.start()
        self.pool.append(process)

def start(leader, replicas, interface=None):
    logger.info("Starting Miyamoto...")
    m = _MiyamotoNode(leader, replicas, interface)
    m.start()
    return m