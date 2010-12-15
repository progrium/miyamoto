import time

class AgendaStoreError(Exception): pass

class Agenda(object):
    """ An agenda is like an append-only list structure that's partitioned by
        "time buckets" (think unrolled linked list). The time-based indexes
        make it easy to query timespans, namely items since and items until,
        which lets it be used for scheduling items in the future. """
    
    retries = 3
    
    def __init__(self, client, prefix, resolution=10, ttl=0):
        self.client = client
        self.prefix = prefix
        self.resolution = resolution
        self.ttl = ttl
    
    def add(self, item, at=None):
        bucket = self._bucket(at or time.time())
        for retry in range(self.retries):
            if self.client.append(bucket, ',%s' % str(item), self.ttl) or \
                self.client.add(bucket, str(item), self.ttl):
                return True
        raise AgendaStoreError()      
    
    def get(self, since=None, until=None):
        since = self._time(since or time.time())
        until = self._time(until or time.time())
        num_buckets = (until - since) / self.resolution
        keys = [self._bucket(since + (i * self.resolution)) for i in range(num_buckets)]
        buckets = self.client.get_multi(keys)
        if buckets:
            return ','.join(buckets).split(',')
    
    def _time(self, time):
        return int(time) - int(time) % self.resolution
    
    def _bucket(self, time):
        return '%s-%s' % (self.prefix, self._time(time))


# in progress flag, remember in node <- cleanup, check if still around
# emitter catches internal jobs

