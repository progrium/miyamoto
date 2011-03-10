import gevent.monkey; gevent.monkey.patch_all()

import socket

import gevent
import gevent.server
import gevent.socket
import gevent.queue

from cluster import ClusterManager
from task import Task
import util

class ScheduleError(Exception): pass

class DistributedScheduler(object):
    def __init__(self, queue, leader, replica_factor=2, replica_offset=5, interface=None, 
        port=6001, cluster_port=6000):
        if interface is None:
            interface = socket.gethostbyname(socket.gethostname())
        self.interface = interface
        self.port = port
        
        self.cluster = ClusterManager(leader, callback=self._cluster_update, 
                            interface=interface, port=cluster_port)
        self.backend = gevent.server.StreamServer((interface, port), self._connection_handler)
        self.hosts = set()
        self.connections = {}
        
        self.queue = queue
        self.scheduled = {}
        self.scheduled_acks = {}
        self.schedules = 0
        
        self.replica_factor = replica_factor
        self.replica_offset = replica_offset
        
    def start(self):
        self.dispatcher = gevent.socket.create_connection((self.interface, 6002), source_address=(self.interface, 0))
        self.backend.start()
        self.cluster.start()
    
    def schedule(self, task):
        host_list = list(self.hosts)
        # This implements the round-robin N replication method for picking
        # which hosts to send the task. In short, every schedule moves along the
        # cluster ring by one, then picks N hosts, where N is level of replication
        replication_factor = min(self.replica_factor, len(host_list))
        host_ids = [(self.schedules + n) % len(host_list) for n in xrange(replication_factor)]
        hosts = [host_list[id] for id in host_ids]
        task.replica_hosts = hosts
        self.scheduled_acks[task.id] = gevent.queue.Queue()
        for host in hosts:
            self.connections[host].send('schedule:%s\n' % task.serialize())
            task.replica_offset += self.replica_offset
        try:
            # TODO: document, wrap this whole operation in timeout
            return all([self.scheduled_acks[task.id].get(timeout=2) for h in hosts])
        except gevent.queue.Empty:
            raise ScheduleError("not all hosts acked")
        finally:
            self.schedules += 1 
            del self.scheduled_acks[task.id]
    
    def cancel(self, task):
        other_replica_hosts = set(task.replica_hosts) - set([self.interface])
        for host in other_replica_hosts:
            if host in self.connections:
                self.connections[host].send('cancel:%s\n' % task.id)
    
    def _cluster_update(self, hosts):
        add_hosts = hosts - self.hosts
        remove_hosts = self.hosts - hosts
        for host in remove_hosts:
            gevent.spawn(self.disconnect, host)
        for host in add_hosts:
            gevent.spawn(self.connect, host)
        self.hosts = hosts
    
    def connect(self, host):
        print "connecting to %s:%s" % (host, self.port)
        client = gevent.socket.create_connection((host, self.port), source_address=(self.interface, 0))
        self.connections[host] = client
        for line in util.line_protocol(client):
            ack, task_id = line.split(':', 1)
            try:
                self.scheduled_acks[line].put(True)
            except KeyError:
                pass
        if host in self.connections:
            print "disconnected from %s" % host
            del self.connections[host]
    
    def disconnect(self, host):
        if host in self.connections:
            print "disconnecting from %s" % host
            self.connections[host].shutdown(0)
            del self.connections[host]
    
    def _enqueue(self, task, serialized):
        self.dispatcher.send('%s\n' % serialized)
        del self.scheduled[task.id]
    
    def _connection_handler(self, socket, address):
        for line in util.line_protocol(socket):
            action, payload = line.split(':', 1)
            if action == 'schedule':
                task = Task.unserialize(payload)
                #print "scheduled: %s" % task.id
                self.scheduled[task.id] = gevent.spawn_later(task.time_until(), self._enqueue, task, payload)
                socket.send('scheduled:%s\n' % task.id)
            elif action == 'cancel':
                task_id = payload
                print "canceled: %s" % task_id
                self.scheduled[task_id].pop().kill()
                #socket.send('%s\n' % task_id)
            elif action == 'delay':
                print "internal delay"
                task_id = payload
                self.scheduled[task_id].start_later() # ummm
                #socket.send('%s\n' % task_id)   
