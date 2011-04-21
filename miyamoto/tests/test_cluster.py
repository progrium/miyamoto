import multiprocessing

import gevent
from nose import with_setup

from miyamoto.test import Cluster
from miyamoto.test import TaskCountdown
from miyamoto import constants

_cluster = Cluster(size=2, replica_factor=2)

def start_cluster():
    _cluster.start()

def stop_cluster():
    _cluster.stop()

@with_setup(start_cluster, stop_cluster)
def test_cluster_starts_and_all_nodes_work():
    countdown = TaskCountdown(len(_cluster))
    url = countdown.start()
    for node in _cluster.nodes:
        node.enqueue('test', {'url': url})
    countdown.wait(1)
    assert countdown.finished()

@with_setup(start_cluster, stop_cluster)
def test_replication_by_killing_node_after_schedule():
    countdown = TaskCountdown(1)
    url = countdown.start()
    assert _cluster.nodes[0].enqueue('test', {'url': url, 'countdown': 1})
    assert not countdown.finished()
    _cluster.nodes[0].terminate()
    countdown.wait(1)
    assert countdown.finished()

@with_setup(start_cluster, stop_cluster)
def test_add_node_and_that_it_replicates_into_cluster():
    countdown = TaskCountdown(2)
    url = countdown.start()
    node = _cluster.add()
    gevent.sleep(2)
    print url
    assert node.enqueue('test', {'url': url})
    assert node.enqueue('test', {'url': url})
    #node.enqueue('test', {'url': url, 'countdown': 2})
    #node.terminate()
    countdown.wait(2)
    print countdown.count
    assert countdown.finished()

def test_killing_the_leader_means_nothing():
    # start cluster
    # kill leader
    # add node
    # hit nodes twice
    # assert tasks
    pass


