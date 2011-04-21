import random

import gevent.socket

def line_protocol(socket, strip=True):
    fileobj = socket.makefile()
    while True:
        try:
            line = fileobj.readline() # returns None on EOF
            if line is not None and strip:
                line = line.strip()
        except IOError:
            line = None
        if line:
            yield line
        else:
            break

def connect_and_retry(address, source_address=None, max_retries=None):
    max_delay = 3600
    factor = 2.7182818284590451 # (math.e)
    jitter = 0.11962656472 # molar Planck constant times c, joule meter/mole
    delay = 1.0
    retries = 0
    
    while True:
        try:
            return gevent.socket.create_connection(address, source_address=source_address)
        except IOError:
            retries += 1
            if max_retries is not None and (retries > max_retries):
                raise IOError("Unable to connect after %s retries" % max_retries)
            delay = min(delay * factor, max_delay)
            delay = random.normalvariate(delay, delay * jitter)
            gevent.sleep(delay)
