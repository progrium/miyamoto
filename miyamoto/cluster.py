""" distributed group membership module

This provides distributed group membership for easily building clustered
applications with gevent. Using this in your app, you just provide the IP
of another node in the cluster and it will receive the IPs of all nodes in
the cluster. When a node joins or drops from the cluster, all other nodes find
out immediately.

The roster is managed by a leader. When you create a cluster, you tell the
first node it is the leader (by simply pointing it to its own IP). As you
add nodes, you can point them to the leader or any other node. If a node
is not the leader, it will redirect the connection to the leader. All nodes
also maintain a keepalive with the leader.

If the leader drops from the cluster, the nodes will dumbly pick a new leader
by taking the remaining node list, sorting it, and picking the first node. If
a node happens to get a different leader, as long as it is in the cluster, it
will be redirected to the right leader. 

To try it out, you need to make several more loopback interfaces:

In OSX:
 ifconfig lo0 inet 127.0.0.2 add
 ifconfig lo0 inet 127.0.0.3 add
 ifconfig lo0 inet 127.0.0.4 add

In Linux:
 ifconfig lo:2 127.0.0.2 up
 ifconfig lo:3 127.0.0.3 up
 ifconfig lo:4 127.0.0.4 up
 
Now you can start the first node on 127.0.0.1:
 python cluster.py 127.0.0.1 127.0.0.1

The first argument is the leader, the second is the interface to bind to.
 
Start the others pointing to 127.0.0.1:
 python cluster.py 127.0.0.1 127.0.0.2
 python cluster.py 127.0.0.1 127.0.0.3

Try starting the last one pointing to a non-leader:
 python cluster.py 127.0.0.3 127.0.0.4

Now you can kill any node (including the leader) and bring up another node 
pointing to any other node, and they all get updated immediately.

"""
import socket
import json

import gevent
import gevent.monkey
import gevent.server
import gevent.socket

gevent.monkey.patch_socket()


class NewLeader(Exception): pass

class ClusterManager(object):
    def __init__(self, leader, callback=None, interface=None, port=6000):
        if interface is None:
            interface = socket.gethostbyname(socket.gethostname())
        self.interface = interface
        self.leader = leader
        self.callback = callback
        self.port = port
        self.cluster = set()
        self.server = None
        self.connections = {}
    
    def is_leader(self):
        return (self.interface == self.leader)
    
    def start(self):
        self.server = gevent.server.StreamServer((self.interface, self.port), self._connection_handler)
        self.server.start()
        if self.is_leader():
            self.cluster.add(self.interface)
            if self.callback:
                self.callback(set(self.cluster))
        else:
            gevent.spawn(self.connect)
    
    def connect(self):
        """ 
        Connects to the currently known leader. It maintains a connection expecting
        JSON lists of hosts in the cluster. It should receive a list on connection,
        however, if a list of one, this is a redirect to the leader (you hit a node
        in the cluster that's not the leader). We also maintain a keepalive. If we
        disconnect, it does a leader elect and reconnects.
        """
        client = gevent.socket.create_connection((self.leader, self.port), 
                    source_address=(self.interface, 0))
        fileobj = client.makefile()
        keepalive = gevent.spawn_later(5, lambda: client.send('\n'))
        try:
            while True:
                try:
                    line = fileobj.readline()
                except IOError:
                    line = None
                if line:
                    if line == '\n':
                        # Keepalive ack from leader
                        keepalive.kill()
                        keepalive = gevent.spawn_later(5, lambda: client.send('\n'))
                    else:
                        new_cluster = json.loads(line)
                        if len(new_cluster) == 1:
                            # Cluster of one means you have the wrong leader
                            self.leader = new_cluster[0]
                            print "redirected to %s..." % self.leader
                            raise NewLeader()
                        else:
                            self.cluster = set(new_cluster)
                            if self.callback:
                                self.callback(set(self.cluster))
                else:
                    self.cluster.remove(self.leader)
                    candidates = list(self.cluster)
                    candidates.sort()
                    self.leader = candidates[0]
                    print "new leader %s..." % self.leader
                    # TODO: if i end up thinking i'm the leader when i'm not
                    # then i will not rejoin the cluster
                    raise NewLeader()
        except NewLeader:
            if self.callback:
                self.callback(set(self.cluster))
            if not self.is_leader():
                gevent.sleep(1)
                self.connect()

    def _connection_handler(self, socket, address):
        """
        If not a leader, a node will simply return a single item list pointing
        to the leader. Otherwise, it will add the host of the connected client
        to the cluster roster, broadcast to all nodes the new roster, and wait
        for keepalives. If no keepalive within timeout or the client drops, it
        drops it from the roster and broadcasts to all remaining nodes. 
        """
        #print 'New connection from %s:%s' % address
        if not self.is_leader():
            socket.send(json.dumps([self.leader]))
            socket.close()
        else:
            self._update(add=(address[0], socket))
        
            timeout = gevent.spawn_later(10, lambda: socket.shutdown(0))            
            fileobj = socket.makefile()
            while True:
                try:
                    line = fileobj.readline()
                except IOError:
                    line = None
                timeout.kill()
                if not line:
                    #print "client disconnected"
                    self._update(remove=address[0])
                    break
                else:
                    timeout = gevent.spawn_later(10, lambda: socket.shutdown(0))
                    socket.send('\n')
                    #print "keepalive from %s:%s" % address
    
    def _update(self, add=None, remove=None):
        """ Used by leader to manager and broadcast roster """
        if add:
            self.cluster.add(add[0])
            self.connections[add[0]] = add[1]
        if remove:
            self.cluster.remove(remove)
            del self.connections[remove]
        for conn in self.connections:
            self.connections[conn].send('%s\n' % json.dumps(list(self.cluster)))
        if self.callback:
            self.callback(set(self.cluster))


if __name__ == '__main__':
    import sys
    
    leader = sys.argv[1]
    interface = sys.argv[2] if len(sys.argv) == 3 else None
    
    def print_cluster(cluster):
        print json.dumps(list(cluster))
    
    print "%s: Using leader %s..." % (interface, leader)
    
    ClusterManager(leader, callback=print_cluster, interface=interface).start()

    while True:
        gevent.sleep()