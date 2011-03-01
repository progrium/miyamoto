import base64
import cPickle
import json
import time
import urllib
import urllib2
import urlparse
import uuid


class Task(object):
    def __init__(self, queue_name, content_type, body):
        self.queue_name = queue_name
        if content_type == 'application/json':
            data = json.loads(body)
            self.url = data['url']
            self.method = data.get('method', 'POST')
            self.countdown = data.get('countdown')
            self.eta = data.get('eta')
            self.params = data.get('params', {})
        elif content_type == 'application/x-www-form-urlencoded':
            data = urlparse.parse_qs(body)
            self.url = data['task.url'][0]
            self.method = data.get('task.method', ['POST'])[0]
            self.countdown = data.get('task.countdown', [None])[0]
            self.eta = data.get('task.eta', [None])[0]
            self.params = dict([(k,v[0]) for k,v in data.items() if not k.startswith('task.')])
        else:
            raise NotImplementedError("content type not supported")
        if self.countdown and not self.eta:
            self.eta = int(time.time()+self.countdown)
        self.id = str(uuid.uuid4())
        self.replica_hosts = []
        self.replica_offset = 0
    
    def time_until(self):
        if self.countdown:
            return int(self.countdown) + self.replica_offset
        elif self.eta:
            countdown = int(int(self.eta)-time.time())
            if countdown < 0:
                return 0 + self.replica_offset
            else:
                return countdown + self.replica_offset
        else:
            return 0 + self.replica_offset
    
    def request(self):
        if self.method == 'POST':
            return urllib2.Request(self.url, urllib.urlencode(self.params))
        elif task.method == 'GET':
            if self.params:
                return urllib2.Request('?'.join([self.url, urllib.urlencode(self.params)]))
            else:
                return urllib2.Request(self.url)
        else:
            return None
    
    def serialize(self):
        return base64.b64encode(cPickle.dumps(self))
    
    @classmethod
    def unserialize(cls, data):
        return cPickle.loads(base64.b64decode(data))