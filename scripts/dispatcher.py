import socket
try:
    import json
except ImportError:
    import simplejson as json

import gevent
import gevent.monkey
import gevent.server
import gevent.socket

gevent.monkey.patch_socket()

from restkit.manager.mgevent import GeventManager
from restkit.globals import set_manager
set_manager(GeventManager())
from restkit import request


def handler(socket, address):
    print "new connection"
    fileobj = socket.makefile()
    while True:
        try:
            line = fileobj.readline().strip()
        except IOError:
            line = None
        if line:
            gevent.spawn(request, line, method="POST")
        else:
            break
    print "conn drop"

server = gevent.server.StreamServer(('127.0.0.1', 6002), handler)
server.start()

while True:
    gevent.sleep(1)