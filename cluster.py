import sys
import os
import random
import eventlet
import collections

from eventlet.green import socket
from eventlet.green import zmq
from eventlet.hubs import use_hub
from zmq import devices

import memcache

use_hub('zeromq')

class CoordinationError(Exception): pass

class ClusterNode(object):
    """ This is a simple cluster management system using memcache to coordinate.
        It uses heartbeats to keep the local cluster representation fresh while
        refreshing the node's record in memcache for the others. If a node
        doesn't heartbeat before its TTL, it will be dropped from peers when
        they heartbeat. """
    
    retries = 3
    
    def __init__(self, context, client, prefix, id, ttl=30, port=7777):
        self.context = context
        self.client = client
        self.index = prefix
        self.id = id
        self.ttl = ttl
        self.port = port
        self.cluster = set()
        self.control = None
        self.callbacks = collections.defaultdict(list)
    
    def join(self):
        for retry in range(self.retries):
            cluster = self.client.gets(self.index) or ''
            if cluster:
                cluster = self._cleanup_index(set(cluster.split(',')))
            else:
                cluster = set()
            self.client.set('%s.%s' % (self.index, self.id), self.id, time=self.ttl)
            cluster.add(self.id)
            if self.client.cas(self.index, ','.join(cluster)):
                print "[Cluster] Joined as %s" % self.id
                self.cluster = cluster
                self._schedule_heartbeat()
                self._create_sockets(self.cluster)
                return True
        raise CoordinationError()
    
    def leave(self):
        for retry in range(self.retries):
            self.client.delete('%s.%s' % (self.index, self.id))
            cluster = self.client.gets(self.index) or ''
            if cluster:
                cluster = self._cleanup_index(set(cluster.split(',')))
            else:
                cluster = set()
            if self.client.cas(self.index, ','.join(cluster)):
                self.cluster = set()
                return True
        raise CoordinationError()
    
    def peers(self):
        return list(self.cluster - set([self.id]))
    
    def all(self):
        return list(self.cluster)
    
    def _cleanup_index(self, cluster):
        index = self.client.get_multi(cluster, '%s.' % self.index) 
        return set([n for n in index if n])
    
    def _create_sockets(self, addresses=None):
        self.control = self.context.socket(zmq.SUB)
        self.control.setsockopt(zmq.SUBSCRIBE, '')
        self._control_out = self.context.socket(zmq.PUB)
        if addresses:
            self._connect_sockets(addresses)
    
    def _connect_sockets(self, addresses):
        for address in addresses:
            self.control.connect('tcp://%s:%s' % (address, self.port))
        
    def send(self, message):
        self._control_out.send(message)
    
    def _schedule_heartbeat(self):
        if len(self.cluster):
            random_interval = random.randint(self.ttl/2, self.ttl-(self.ttl/5))
            def update():
                self.client.set('%s.%s' % (self.index, self.id), self.id, time=self.ttl)
                old_cluster = self.cluster
                self.cluster = self._cleanup_index(set(self.client.get(self.index).split(',')))
                print self.cluster
                added = self.cluster - old_cluster
                if added:
                    print "[Cluster] Added %s" % ', '.join(added)
                    self._connect_sockets(added)
                    for cb in self.callbacks['add']:
                        cb(added)
                self._schedule_heartbeat()
            eventlet.spawn_after(random_interval, update)

