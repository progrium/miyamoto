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

from utils import Device
from cluster import ClusterNode
from agenda import Agenda

use_hub('zeromq')

interface = sys.argv[1]

cluster_address, cluster_key = sys.argv[2].split('/')

datastore = memcache.Client([cluster_address], debug=0)

ctx = zmq.Context()

cluster = ClusterNode(ctx, datastore, cluster_key, interface, ttl=10)
cluster.join()

agenda = Agenda(datastore, 'miyamoto-data')

dispatch_timeout = 60

enqueued = set()

frontend = Device(zmq.QUEUE, zmq.XREP, zmq.XREQ, ctx)
frontend.bind_in('tcp://%s:7000' % interface)
frontend.bind_out('inproc://frontend-out')
print "Starting frontend on 7000..."
frontend.start()

# token::{task}
# :time:{task}
# token:time:{task}
# ::{task}
# {task}
def enqueuer():
    incoming = ctx.socket(zmq.REP)
    incoming.connect('inproc://frontend-out')
    outgoing = ctx.socket(zmq.PUSH)
    def connect(node):
        print "Enqueuer connecting to %s..." % node
        if node == interface:
            outgoing.connect('inproc://dispatcher-in')
        else:
            outgoing.connect('tcp://%s:7050' % node)
    for node in cluster.all():
        connect(node)
    cluster.callbacks['add'].append(lambda x: map(connect,x))
    while True:
        msg = incoming.recv()
        params, task = msg.split('{', 1)
        task = '{%s' % task
        if params:
            token, time, x = params.split(':')
        else:
            token = None
            time = None
        if token and token in enqueued:
            incoming.send('{"status":"duplicate"}')
            continue
        id = uuid.uuid4().hex
        if agenda.add(id, at=time or None) and datastore.set(id, task):
            incoming.send('{"status":"stored", "id": "%s"}' % id)
            if token:
                enqueued.add(token)
                cluster.send('enqueue:%s' % token)
            outgoing.send_pyobj((id, task))
        else:
            incoming.send('{"status":"failure"}')
        
def dispatcher():
    incoming = ctx.socket(zmq.PULL)
    incoming.bind('tcp://%s:7050' % interface)
    incoming.bind('inproc://dispatcher-in')
    outgoing = ctx.socket(zmq.PUSH)
    print "Binding dispatcher on 8000..."
    outgoing.bind('tcp://%s:8000' % interface)
    while True:
        id, task = incoming.recv_pyobj()
        if not datastore.get('%s-dispatch' % id):
            outgoing.send("%s:%s" % (id, task))
            datastore.set('%s-dispatch' % id, dispatch_timeout)

def finisher():
    incoming = ctx.socket(zmq.SUB)
    incoming.setsockopt(zmq.SUBSCRIBE, "")
    print "Binding finisher on 9000..."
    incoming.bind('tcp://%s:9000' % interface)
    while True:
        id = incoming.recv()
        datastore.delete('%s-dispatch' % id)
        datastore.delete(id)

def control():
    while True:
        cmd, payload = cluster.control.recv().split(',')
        if cmd == 'enqueue':
            enqueued.add(payload)

try:
    for n in range(2):
        eventlet.spawn_after(1, enqueuer)
    eventlet.spawn_n(dispatcher)
    eventlet.spawn_n(finisher)
    
    while True:
        eventlet.sleep(1)
        
except (KeyboardInterrupt, SystemExit):
    print "Exiting..."
    cluster.leave()
    ctx.term()