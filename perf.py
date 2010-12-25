import sys
import os
import eventlet
import uuid
import time
import random

from eventlet.green import socket
from eventlet.green import zmq
from eventlet.hubs import use_hub
from zmq import devices

import memcache

import utils

use_hub('zeromq')

task = sys.argv[1]
nodes = utils.cluster(sys.argv[2])
ctx = zmq.Context()


messages = []

def stopped(count, td):
    print count
    print td
    print 1/(td/count)
    sys.exit(0)

def enqueuer(n):
    frontend = ctx.socket(zmq.REQ)
    for node in nodes:
        frontend.connect('tcp://%s:7000' % node)
    for m in range(n):
        job = '%s::{"job":%s}' % (random.random(), time.time())
        frontend.send(job)
        resp = frontend.recv()
        messages.append(job)
        #print resp

def dequeuer():
    dispatcher = ctx.socket(zmq.PULL)
    finisher = ctx.socket(zmq.PUB)
    for node in nodes:
        dispatcher.connect('tcp://%s:8000' % node)
        finisher.connect('tcp://%s:9000' % node)
    timeout = None
    t1 = None
    while True:
        id, job = dispatcher.recv().split(':', 1)
        if not timeout is None:
            timeout.cancel()
        if not t1:
            t1 = time.time()
        messages.append(job)
        finisher.send(id)
        td = time.time() - t1
        timeout = eventlet.spawn_after(5, stopped, len(messages), td)
        
        

try:
    if task == 'enqueue':
        size = int(os.environ.get('MESSAGES', 10000))
        t1 = time.time()
        eventlet.spawn_n(enqueuer, size)
        while len(messages) < size:
            eventlet.sleep(0.1)
        td = time.time() - t1
        stopped(len(messages), td)
        
    elif task == 'dequeue':
        eventlet.spawn_n(dequeuer)
        while True:
            eventlet.sleep(0.1)
        
except (KeyboardInterrupt, SystemExit):
    sys.exit(0)