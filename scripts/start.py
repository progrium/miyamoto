import sys
import os

from gevent_zeromq import zmq
import gevent

from miyamoto.queue import QueueServer
from miyamoto.dispatcher import Dispatcher

interface = os.environ.get('INTERFACE')
leader = os.environ.get('LEADER', interface)
replicas = int(os.environ.get('REPLICAS', 2))

print "%s: Using leader %s..." % (interface, leader)

if os.fork():
    print "Starting queue server..."
    gevent.sleep(1)
    server = QueueServer(leader, replica_factor=replicas, interface=interface)
    server.run()
else:
    ctx = zmq.Context()
    dispatcher = Dispatcher(interface, ctx)
    dispatcher.start()