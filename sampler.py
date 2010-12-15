import sys
import os
import eventlet
import time

from eventlet.green import socket
from eventlet.green import zmq
from eventlet.hubs import use_hub

import utils

use_hub('zeromq')

ctx = zmq.Context()

nodes = utils.cluster(sys.argv[1])


def sampler(rate):
    dispatcher = ctx.socket(zmq.PULL)
    finisher = ctx.socket(zmq.PUB)
    for node in nodes:
        dispatcher.connect("tcp://%s:8000" % node)
        finisher.connect("tcp://%s:9000" % node)
    while True:
        id, job = dispatcher.recv().split(':', 1)
        rate.tick()
        finisher.send(id)

def redraw(v, t):
    os.system("clear")
    print "Last sample: %s messages/sec" % v

try:
    rate = utils.SampledRate(0.5, 3, callback=redraw, name='messages')
    eventlet.spawn_n(sampler, rate)
        
    while True:
        eventlet.sleep(1)
        
except (KeyboardInterrupt, SystemExit):
    print "Exiting."