import sys
import os

from miyamoto import start

interface = os.environ.get('INTERFACE')
leader = os.environ.get('LEADER', interface)
replicas = int(os.environ.get('REPLICAS', 2))

print "%s: Using leader %s..." % (interface, leader)

start(leader, replicas, interface)
