import sys
import os
import eventlet
import uuid
import time
import json

from eventlet.green import socket
from eventlet.green import zmq
from eventlet.hubs import use_hub
from eventlet import wsgi
from zmq import devices

import memcache
import webob

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

# These need to have a max size
enqueued = set()

frontend = Device(zmq.QUEUE, zmq.XREP, zmq.XREQ, ctx)
frontend.bind_in('tcp://%s:7000' % interface)
frontend.bind_out('inproc://frontend-out')
print "Starting frontend on 7000..."
frontend.start()

enqueue_out = ctx.socket(zmq.PUSH)

# token::{task}
# :time:{task}
# token:time:{task}
# ::{task}
# {task}
def enqueuer():
    incoming = ctx.socket(zmq.REP)
    incoming.connect('inproc://frontend-out')
    
    while True:
        msg = incoming.recv()
        params, task = msg.split('{', 1)
        task = '{%s' % task
        if params:
            token, time, x = params.split(':')
        else:
            token = None
            time = None

        id = enqueue(task, time, token)
        if id:
            incoming.send('{"status":"stored", "id": "%s"}' % id)
        elif id == False:
            incoming.send('{"status":"duplicate"}')
        else:
            incoming.send('{"status":"failure"}')

def enqueue(task, time=None, token=None):
    if token and token in enqueued:
        return False
    id = uuid.uuid4().hex
    if agenda.add(id, at=time or None) and datastore.set(id, task):
        if token:
            enqueued.add(token)
        enqueue_out.send_pyobj((id, task))
        return id
    else:
        return None
        
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
        # ...

def web_enqueuer(env, start_response):
    req = webob.Request(env)
    task = dict(req.POST)
    if '_time' in task:
        del task['_time']
    if '_token' in task:
        del task['_token']
    task = json.dumps(task)
    id = enqueue(task, req.POST.get('_time'), req.POST.get('_token'))
    if id:
        start_response('200 OK', [('Content-Type', 'application/json')])
        return ['{"status":"stored", "id": "%s"}\n' % id]
        outgoing.send_pyobj((id, task))
    elif id == False:
        start_response('400 Bad request', [('Content-Type', 'application/json')])
        return ['{"status":"duplicate"}\n']
    else:
        start_response('500 Server error', [('Content-Type', 'application/json')])
        return ['{"status":"failure"}\n']
    

def setup_enqueue():
    def connect(node):
        print "Enqueuer connecting to %s..." % node
        if node == interface:
            enqueue_out.connect('inproc://dispatcher-in')
        else:
            enqueue_out.connect('tcp://%s:7050' % node)
    for node in cluster.all():
        connect(node)
    cluster.callbacks['add'].append(lambda x: map(connect,x))

try:
    
    for n in range(2):
        eventlet.spawn_after(1, enqueuer)
    eventlet.spawn_n(dispatcher)
    eventlet.spawn_n(finisher)
    
    eventlet.spawn_after(1, setup_enqueue)
    
    
    wsgi.server(eventlet.listen((interface, 8080)), web_enqueuer)
    #while True:
    #    eventlet.sleep(1)
        
except (KeyboardInterrupt, SystemExit):
    print "Exiting..."
    cluster.leave()
    ctx.term()