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

cluster = ClusterNode(datastore, cluster_key, interface, ttl=10)
cluster.join()

agenda = Agenda(datastore, 'miyamoto-data')

frontend = Device(zmq.QUEUE, zmq.XREP, zmq.XREQ, ctx)
frontend.bind_in('tcp://%s:7000' % interface)
frontend.bind_out('inproc://frontend-out')
print "Starting frontend on 7000..."
frontend.start()


def enqueuer():
    incoming = ctx.socket(zmq.REP)
    incoming.connect('inproc://frontend-out')
    outgoing = ctx.socket(zmq.PUSH)
    def connect(node):
        print "Enqueuer connecting to %s..." % node
        outgoing.connect('tcp://%s:7050' % node)
    for node in cluster.all():
        connect(node)
    cluster.callbacks['add'].append(lambda x: map(connect,x))
    while True:
        task = incoming.recv()
        id = uuid.uuid4().hex
        if agenda.add(id) and datastore.set(id, task):
            incoming.send('{"status":"stored"}')
            #print ">"
            outgoing.send_pyobj((id, task))
        else:
            incoming.send('{"status":"failure"}')
        
def dispatcher():
    incoming = ctx.socket(zmq.PULL)
    incoming.bind('tcp://%s:7050' % interface)
    outgoing = ctx.socket(zmq.PUSH)
    print "Binding dispatcher on 8000..."
    outgoing.bind('tcp://%s:8000' % interface)
    while True:
        id, task = incoming.recv_pyobj()
        if not datastore.get('%s-dispatch' % id):
            outgoing.send("%s:%s" % (id, task))
            #print "<"
            datastore.set('%s-dispatch' % id, time.time())

def finisher():
    incoming = ctx.socket(zmq.SUB)
    incoming.setsockopt(zmq.SUBSCRIBE, "")
    print "Binding dispatcher on 9000..."
    incoming.bind('tcp://%s:9000' % interface)
    while True:
        id = incoming.recv()
        datastore.delete(id)
        #print "x"


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