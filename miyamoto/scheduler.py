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
        
        self.dispatcher = None
        self.cluster = ClusterManager(leader, callback=self._cluster_update, 
                            interface=interface, port=cluster_port)
        self.backend = gevent.server.StreamServer((interface, port), self._backend_server)
        self.hosts = set()
        self.connections = {}
        
        self.queue = queue
        self.scheduled = {}
        self.scheduled_acks = {}
        self.schedules = 0
        
        self.replica_factor = replica_factor
        self.replica_offset = replica_offset
        
    def start(self):
        gevent.spawn(self._dispatcher_client)
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
            self.scheduled_acks.pop(task.id)

    
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
            if ack == 'scheduled' and task_id in self.scheduled_acks:
                self.scheduled_acks[task_id].put(True)
        if host in self.connections:
            print "disconnected from %s" % host
            self.connections.pop(host)
    
    def disconnect(self, host):
        if host in self.connections:
            print "disconnecting from %s" % host
            self.connections.pop(host).shutdown(0)
            
    
    def _dispatch(self, task, serialized):
        self.dispatcher.send('%s\n' % serialized)
        self.scheduled.pop(task.id)
    
    def _dispatcher_client(self):
        while True:
            self.dispatcher = gevent.socket.create_connection((self.interface, 6002), source_address=(self.interface, 0))
            for line in util.line_protocol(self.dispatcher):
                event, payload = line.split(':', 1)
                if event == 'start':
                    task = self.scheduled[payload]
                    when = None #??
                    self._sendto_replicas(task, 'reschedule:%s:%s\n' % (task.id, when))
                elif event == 'success':
                    task = self.scheduled[payload]
                    self._sendto_replicas(task, 'kill:%s\n' % task.id)
                elif event == 'failure':
                    task_id, reason = payload.split(':', 1)
                    
            print "disconnected from dispatcher, retrying..."
    
    def _sendto_replicas(self, task, message):
        other_replica_hosts = set(task.replica_hosts) - set([self.interface])
        for host in other_replica_hosts:
            if host in self.connections:
                self.connections[host].send(message)
    
    def _backend_server(self, socket, address):
        for line in util.line_protocol(socket):
            action, payload = line.split(':', 1)
            if action == 'schedule':
                task = Task.unserialize(payload)
                self.scheduled[task.id] = gevent.spawn_later(task.time_until(), self._dispatch, task, payload)
                socket.send('scheduled:%s\n' % task.id)
            elif action == 'kill':
                task_id = payload
                print "canceled: %s" % task_id
                self.scheduled.pop(task_id).kill()
            elif action == 'reschedule':
                task_id, when = payload.split(':', 1)
                print "rescheduled: %s for %s" % (task_id, when)
                self.scheduled[task_id].start_later() # ummm
