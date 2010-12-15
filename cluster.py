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
    
    def __init__(self, client, prefix, id, ttl=30):
        self.client = client
        self.index = prefix
        self.id = id
        self.ttl = ttl
        self.cluster = set()
        self.callbacks = collections.defaultdict(list)
    
    def join(self):
        for retry in range(self.retries):
            cluster = set((self.client.gets(self.index) or '').split(','))
            cluster = self._cleanup_index(cluster)
            self.client.set('%s.%s' % (self.index, self.id), self.id, time=self.ttl)
            cluster.add(self.id)
            if self.client.cas(self.index, ','.join(cluster)):
                print "[Cluster] Joined as %s" % self.id
                self.cluster = cluster
                self._schedule_heartbeat()
                return True
        raise CoordinationError()
    
    def leave(self):
        for retry in range(self.retries):
            self.client.delete('%s.%s' % (self.index, self.id))
            cluster = set(self.client.gets(self.index).split(','))
            cluster = self._cleanup_index(cluster)
            if self.clients.cas(self.index, ','.join(cluster)):
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
    
    def _schedule_heartbeat(self):
        if len(self.cluster):
            random_interval = random.randint(self.ttl/2, self.ttl)
            def update():
                self.client.set('%s.%s' % (self.index, self.id), self.id, time=self.ttl)
                old_cluster = self.cluster
                self.cluster = self._cleanup_index(set(self.client.get(self.index).split(',')))
                print self.cluster
                added = self.cluster - old_cluster
                if added:
                    print "[Cluster] Added %s" % ', '.join(added)
                    for cb in self.callbacks['add']:
                        cb(added)
                self._schedule_heartbeat()
            eventlet.spawn_after(random_interval, update)

