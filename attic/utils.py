from zmq import devices
import collections
import time
import operator
import memcache

def cluster(spec):
    address, key = spec.split('/')
    return memcache.Client([address]).get(key).split(',')

def elect(client, name, candidate, ttl=60):
    """ Simple leader election-esque distributed selection method """
    if not client.append(name, ',%' % cadidate, time=ttl):
        if client.add(name, candidate, time=ttl):
            return True
        else:
            return False
    else:
        return False

class SampledRate(object):
    """Tool for pushing rate over time data"""
    
    def __init__(self, frequency=1, resolution=1, parent=None, callback=None, name=None):
        """ frequency:  Rate update frequency in seconds
            resolution: Interval to average data over in seconds
            parent:     Another SampledRate that ticks will propagate to
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
    
class Device(devices.ThreadDevice):
    def __init__(self, type, in_type, out_type, ctx):
        self._context = ctx
        devices.ThreadDevice.__init__(self, type, in_type, out_type)
    
    def _setup_sockets(self):        
        # create the sockets
        ins = self._context.socket(self.in_type)
        if self.out_type < 0:
            outs = ins
        else:
            outs = self._context.socket(self.out_type)
        
        # set sockopts (must be done first, in case of zmq.IDENTITY)
        for opt,value in self._in_sockopts:
            ins.setsockopt(opt, value)
        for opt,value in self._out_sockopts:
            outs.setsockopt(opt, value)
        
        for iface in self._in_binds:
            ins.bind(iface)
        for iface in self._out_binds:
            outs.bind(iface)
        
        for iface in self._in_connects:
            ins.connect(iface)
        for iface in self._out_connects:
            outs.connect(iface)
        
        return ins,outs