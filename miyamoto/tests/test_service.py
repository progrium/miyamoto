import gevent

from miyamoto import service

class BasicService(service.Service):
    def __init__(self, name):
        super(BasicService, self).__init__()
        self.name = name
    
    def _start(self):
        return service.READY
    
    def _stop(self):
        pass

class SlowReadyService(BasicService):
    def _start(self):
        self.spawn(self._run)
    
    def _run(self):
        gevent.sleep(0.5)
        self._ready_event.set()

def test_basic_service():
    s = BasicService('test')
    s.start()
    assert s.started == True, "Service is not started"
    assert s.ready == True, "Service is not ready"
    s.stop()
    assert s.started == False, "Service did not stop"

def test_slow_ready_service():
    s = SlowReadyService('test')
    s.start(block_until_ready=False)
    assert s.ready == False, "Service was ready too quickly"
    assert s.started == True, "Service is not started"
    s.stop()
    assert s.ready == False, "Service was still ready after stop"
    assert s.started == False, "Service did not stop"
    
    s.start()
    assert s.ready == True, "Service was not ready after blocking start"
    s.stop()
    assert s.ready == False, "Service was still ready after stop"

def test_child_service():
    class ParentService(BasicService):
        def __init__(self, name):
            super(ParentService, self).__init__(name)
            self.child = SlowReadyService('child')
            self._children.append(self.child)
            
        def _start(self):
            return service.READY
    
    s = ParentService('parent')
    s.start()
    assert s.child.ready == True, "Child service is not ready"
    assert s.ready == True, "Parent service is not ready"
    s.stop()
    assert s.child.started == False, "Child service is still started"
    assert s.child.ready == False, "Child service is still ready"

def test_service_greenlets():
    class GreenletService(BasicService):
        def _start(self):
            for n in xrange(3):
                self.spawn(self._run, n)
            return service.READY

        def _run(self, index):
            while True:
                gevent.sleep(0.1)
    
    s = GreenletService('greenlets')
    s.start()
    for greenlet in s._greenlets:
        assert not greenlet.ready(), "Greenlet is ready when it shouldn't be"
    s.stop()
    for greenlet in s._greenlets:
        assert greenlet.ready(), "Greenlet isn't ready after stop"
                