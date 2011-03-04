import socket
import urllib2
try:
    import json
except ImportError:
    import simplejson as json

import gevent
import gevent.monkey
import gevent.server
import gevent.socket
import gevent.queue

gevent.monkey.patch_socket()

from task import Task

queue = gevent.queue.Queue()


def _dispatch(serialized_task):
    task = Task.unserialize(serialized_task)
    req = task.request()
    if req:
        print task.url
        urllib2.urlopen(req)

def dispatcher():
    while True:
        _dispatch(queue.get())
    

def handler(socket, address):
    print "new connection"
    fileobj = socket.makefile()
    while True:
        try:
            line = fileobj.readline().strip()
        except IOError:
            line = None
        if line:
            queue.put(line)
        else:
            break
    print "conn drop"

for n in range(10):
    gevent.spawn(dispatcher)

server = gevent.server.StreamServer(('127.0.0.1', 6002), handler)
server.start()

while True:
    gevent.sleep(1)