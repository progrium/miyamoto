import collections
import operator
import time
import os

import gevent.pywsgi
from gevent_zeromq import zmq

class RateSampler(object):
    """Tool for pushing rate over time data"""
    
    def __init__(self, frequency=1, resolution=1, parent=None, callback=None, name=None):
        """ frequency:  Rate update frequency in seconds
            resolution: Interval to average data over in seconds
            parent:     Another RateSampler that ticks will propagate to
            callback:   Optional callback when frequency is updated"""
        self.frequency  = frequency
        self.resolution = resolution
        self.parent     = parent
        self.callback   = callback
        self.samples    = collections.defaultdict(int)
        self.ticks      = 0
        self.last_start = None
        self.last_value = 0 
        if not name and parent:
            self.name   = parent.name
        else:
            self.name   = name
    
    def _update(self):
        if self.last_start and int(time.time() - self.last_start) > self.frequency:
            # Add empty samples
            for x in range(self.frequency-len(self.samples)):
                self.samples[x] = 0
            self.last_value = reduce(operator.add, self.samples.values()) / self.resolution / self.frequency
            self.last_start = int(time.time())
            if self.callback:
                # reactor.callLater(0, self.callback, self.last_value, self.ticks)
                self.callback(self.last_value, self.ticks)
            self.ticks      = 0
            self.samples    = collections.defaultdict(int)
    
    def tick(self, ticks=1):
        if not self.last_start:
            self.last_start = int(time.time())
        self._update()
        if self.parent:
            self.parent.tick(ticks)
        self.samples[int(time.time() / self.resolution)] += ticks
        self.ticks += ticks
        return self
    
    def getvalue(self):
        self._update()
        return self.last_value
    
    def __int__(self):
        return self.getvalue()
    
    def __str__(self):
        # Okay, hardcoding 1 sec resolutions for now
        return "%i %s/sec" % (self.getvalue(), self.name or 'ticks')
    
    def __repr__(self):
        return "<SampledRate: %i  avg/%is updated/%is>" % (self.getvalue(), self.frequency, self.resolution)


def redraw(v, t):
    os.system("clear")
    print "Last sample: %s tasks/sec" % v

rate = RateSampler(1, 5, callback=redraw, name='tasks')
ctx = zmq.Context()

def http_sampler(env, start_response):
    rate.tick()
    start_response('200 OK', [])
    return ['ok']

def zmq_sampler():
    socket = ctx.socket(zmq.REP)
    socket.bind('tcp://127.0.0.1:9999')
    while True:
        socket.recv()
        rate.tick()
        socket.send("ok")

gevent.spawn(zmq_sampler)
server = gevent.pywsgi.WSGIServer(('', int(os.environ.get('PORT', 9099))), http_sampler, log=None)
server.serve_forever()
