from gevent import wsgi
import gevent

def outputter(env, start):
    print env
    #gevent.sleep(4)
    start('200 OK', {})
    return ["ok"]

print 'Serving on 8080...'
wsgi.WSGIServer(('', 8080), outputter).serve_forever()