import base64
import cPickle
import json
import time
import urllib
import urllib2
import urlparse
import uuid

import gevent

class Task(object):
    def __init__(self, queue_name, content_type, body):
        self.queue_name = queue_name
        if content_type == 'application/json':
            data = json.loads(body)
            self.url = data['url']
            self.method = data.get('method', 'POST')
            countdown = data.get('countdown')
            self.eta = data.get('eta')
            self.params = data.get('params', {})
        elif content_type == 'application/x-www-form-urlencoded':
            data = urlparse.parse_qs(body)
            self.url = data['task.url'][0]
            self.method = data.get('task.method', ['POST'])[0]
            countdown = data.get('task.countdown', [None])[0]
            self.eta = data.get('task.eta', [None])[0]
            self.params = dict([(k,v[0]) for k,v in data.items() if not k.startswith('task.')])
        else:
            raise NotImplementedError("content type not supported")
        if countdown and not self.eta:
            self.eta = int(time.time()+int(countdown))
        self.id = str(uuid.uuid4()) # vs time.time() is about 100 req/sec slower
        self.replica_hosts = []
        self.replica_offset = 0
        self._greenlet = None
        self._serialize_cache = None
    
    def time_until(self):
        if self.eta:
            countdown = int(int(self.eta) - time.time())
            if countdown < 0:
                return self.replica_offset
            else:
                return countdown + self.replica_offset
        else:
            return self.replica_offset
    
    def schedule(self, dispatcher):
        self._greenlet = gevent.spawn_later(self.time_until(), dispatcher.dispatch, self)
    
    def reschedule(self, dispatcher, eta):
        self.cancel()
        self.eta = eta
        self.schedule(dispatcher)
    
    def cancel(self):
        self._greenlet.kill()
    
    def serialize(self):
        if self._serialize_cache:
            return self._serialize_cache
        else:
            return base64.b64encode(cPickle.dumps(self, cPickle.HIGHEST_PROTOCOL))
    
    @classmethod
    def unserialize(cls, data):
        task = cPickle.loads(base64.b64decode(data))
        task._serialize_cache = data
        return task