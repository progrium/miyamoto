import sys
import eventlet

from eventlet.green import socket
from eventlet.green import zmq
from eventlet.hubs import use_hub

import utils

use_hub('zeromq')

ctx = zmq.Context()

nodes = utils.cluster(sys.argv[1])

def worker():
    dispatcher = ctx.socket(zmq.PULL)
    finisher = ctx.socket(zmq.PUB)
    for node in nodes:
        dispatcher.connect('tcp://%s:8000' % node)
        finisher.connect('tcp://%s:9000' % node)
    while True:
        id, job = dispatcher.recv().split(':', 1)
        print job
        finisher.send(id)
        

try:
    eventlet.spawn_n(worker)
    while True: eventlet.sleep(1)
        
except (KeyboardInterrupt, SystemExit):
    print "Exiting."