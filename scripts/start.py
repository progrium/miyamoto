import sys
import os

import gevent

from miyamoto.queue import QueueServer
from miyamoto.dispatcher import Dispatcher

interface = os.environ.get('INTERFACE')
leader = os.environ.get('LEADER', interface)

print "%s: Using leader %s..." % (interface, leader)

if os.fork():
    print "Starting queue server..."
    gevent.sleep(1)
    server = QueueServer(leader, replica_factor=2, interface=interface)
    server.run()
else:
    dispatcher = Dispatcher()
    dispatcher.start()