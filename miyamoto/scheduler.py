import gevent.monkey; gevent.monkey.patch_all()

import socket
import time

import gevent
import gevent.server
import gevent.socket
import gevent.queue

from cluster import ClusterManager
from dispatcher import DispatchClient
from task import Task
import util
import constants

class ScheduleError(Exception): pass

class DistributedScheduler(object):
    def __init__(self, queue, leader, replica_factor=2, replica_offset=5, interface=None, 
        port=6001, cluster_port=6000):
        if interface is None:
            interface = socket.gethostbyname(socket.gethostname())
        self.interface = interface
        self.port = port
        
        self.dispatcher = DispatchClient(interface, self._dispatcher_event)
        self.cluster = ClusterManager(leader, callback=self._cluster_update, 
                            interface=interface, port=cluster_port)
        self.backend = gevent.server.StreamServer((interface, port), self._backend_server)
        self.peers = set()
        self.connections = {}
        
        self.queue = queue
        self.scheduled = {}
        self.scheduled_acks = {}
        self.schedules = 0
        
        self.replica_factor = replica_factor
        self.replica_offset = replica_offset
        
        # Set this semaphore slot to be signaled when a certain num of peers are connected
        self._ready_event = None
        
    def start(self):
        self.dispatcher.start()
        self.backend.start()
        self.cluster.start()
        
        if self.cluster.is_leader() and self._ready_event:
            self._ready_event.set()
    
    def schedule(self, task):
        host_list = list(self.peers)
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
        add_hosts = hosts - self.peers
        remove_hosts = self.peers - hosts
        for host in remove_hosts:
            print "disconnecting from peer %s" % host
            gevent.spawn(self._remove_peer, host)
        for host in add_hosts:
            print "connecting to peer %s" % (host)
            gevent.spawn(self._add_peer, host)
        self.peers = hosts
    
    def _add_peer(self, host):
        client = gevent.socket.create_connection((host, self.port), source_address=(self.interface, 0))
        self.connections[host] = client
        if host == self.cluster.leader and self._ready_event:
            self._ready_event.set()
        for line in util.line_protocol(client):
            ack, task_id = line.split(':', 1)
            if ack == 'scheduled' and task_id in self.scheduled_acks:
                self.scheduled_acks[task_id].put(True)
        print "disconnected from peer %s" % host
        self._remove_peer(host)
    
    def _remove_peer(self, host):
        if host in self.connections:
            peer = self.connections.pop(host)
            try:
                peer.shutdown(0)
            except:
                pass

    def _dispatcher_event(self, event, payload):
        if event == 'start':
            task = self.scheduled[payload]
            eta = int(time.time() + constants.WORKER_TIMEOUT)
            self._sendto_replicas(task, 'reschedule:%s:%s\n' % (task.id, eta))
        
        elif event == 'success':
            task = self.scheduled[payload]
            self._sendto_replicas(task, 'cancel:%s\n' % task.id)
            self.scheduled.pop(task.id)
        
        elif event == 'failure':
            task_id, reason = payload.split(':', 1)
            self.scheduled.pop(task.id)
            print "FAILURE %s: %s" % (task_id, reason)
    
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
                task.schedule(self.dispatcher)
                self.scheduled[task.id] = task
                socket.send('scheduled:%s\n' % task.id)
                print "scheduled: %s" % task.id
            
            elif action == 'cancel':
                task_id = payload
                print "canceled: %s" % task_id
                self.scheduled.pop(task_id).cancel()
            
            elif action == 'reschedule':
                task_id, eta = payload.split(':', 1)
                eta = int(eta)
                print "rescheduled: %s for %s" % (task_id, eta)
                self.scheduled[task_id].reschedule(self.dispatcher, eta)
