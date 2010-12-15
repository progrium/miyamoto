import sys
import os
import eventlet
import uuid
import time

from eventlet.green import socket
from eventlet.green import zmq
from eventlet.hubs import use_hub
from zmq import devices

import memcache

import utils

use_hub('zeromq')

ctx = zmq.Context()

nodes = utils.cluster(sys.argv[1])

messages = []

def enqueuer(n):
    frontend = ctx.socket(zmq.REQ)
    for node in nodes:
        frontend.connect('tcp://%s:7000' % node)
    for m in range(n):
        job = '{"job":%s}' % time.time()
        frontend.send(job)
        resp = frontend.recv()
        messages.append(job)
        #print resp

def worker():
    dispatcher = ctx.socket(zmq.PULL)
    finisher = ctx.socket(zmq.PUB)
    for node in nodes:
        dispatcher.connect('tcp://%s:8000' % node)
        finisher.connect('tcp://%s:9000' % node)
    while True:
        id, job = dispatcher.recv().split(':', 1)
        #print job
        messages.append(job)
        finisher.send(id)
        

try:
    size = 10000
    #eventlet.spawn_n(worker)
    t1 = time.time()
    eventlet.spawn_n(enqueuer, size)
    while len(messages) < size:
        eventlet.sleep(0.1)
    td = time.time() - t1
    print td
    print 1/(td/(size))
        
except (KeyboardInterrupt, SystemExit):
    print "Exiting."