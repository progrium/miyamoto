import cgi
import sys
import eventlet
from eventlet.green import socket
from eventlet.green import zmq
from eventlet.hubs import use_hub

from eventlet import wsgi

import utils

use_hub('zeromq')

port = int(sys.argv[1])
nodes = utils.cluster(sys.argv[2])


ctx = zmq.Context()

pool = []
for n in range(2):
    enqueuer = ctx.socket(zmq.REQ)
    for node in nodes:
        print "Connecting to %s..." % node
        enqueuer.connect('tcp://%s:7000' % node)
    pool.append(enqueuer)

def enqueue(env, start_response):
    for n in range(len(pool)):
        try:
            pool[n].send(env['wsgi.input'].read())
            resp = pool[n].recv()
            if 'stored' in resp:
                start_response('200 OK', [('Content-Type', 'text/plain')])
            else:
                start_response('503 Error', [('Content-Type', 'text/plain')])
            return ['%s\r\n' % resp]
        except:
            pass
    start_response('503 Error', [('Content-Type', 'text/plain')])
    return ['%s\r\n' % resp]

wsgi.server(eventlet.listen(('', port)), enqueue)