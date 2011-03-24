import gevent
import gevent.baseserver
import gevent.event
import gevent.pool

READY = True

class Service(object):
    """Service interface for creating standalone or composable services
    
    This is similar to a subset of the gevent baseserver interface (intentional)
    so that you can treat them as children services. 
    """
    stop_timeout = 1
    ready_timeout = 2
    
    def __init__(self):
        self._stopped_event = gevent.event.Event()
        self._ready_event = gevent.event.Event()
        self._children = []
        self._greenlets = gevent.pool.Group()
        self.started = False
    
    @property
    def ready(self):
        return self._ready_event.isSet()
    
    def spawn(self, *args, **kwargs):
        self._greenlets.spawn(*args, **kwargs)
    
    def start(self, block_until_ready=True):
        assert not self.started, '%s already started' % self.__class__.__name__
        self._stopped_event.clear()
        self._ready_event.clear()
        try:
            for child in self._children:
                if isinstance(child, Service):
                    child.start(block_until_ready)
                elif isinstance(child, gevent.baseserver.BaseServer):
                    child.start()
            ready = self._start()
            if ready is True:
                self._ready_event.set()
            elif not ready and block_until_ready is True:
                self._ready_event.wait(self.ready_timeout)
            self.started = True
        except:
            self.stop()
            raise
    
    def _start(self):
        raise NotImplementedError()
    
    def stop(self, timeout=None):
        """Stop accepting the connections and close the listening socket.

        If the server uses a pool to spawn the requests, then :meth:`stop` also waits
        for all the handlers to exit. If there are still handlers executing after *timeout*
        has expired (default 1 second), then the currently running handlers in the pool are killed."""
        self.started = False
        try:
            for child in self._children:
                child.stop()
            self._stop()
        finally:
            if timeout is None:
                timeout = self.stop_timeout
            if self._greenlets:
                self._greenlets.join(timeout=timeout)
                self._greenlets.kill(block=True, timeout=1)
            self._ready_event.clear()
            self._stopped_event.set()
    
    def _stop(self):
        raise NotImplementedError()
    
    def serve_forever(self, stop_timeout=None):
        """Start the service if it hasn't been already started and wait until it's stopped."""
        if not self.started:
            self.start()
        try:
            self._stopped_event.wait()
        except:
            self.stop(timeout=stop_timeout)
            raise