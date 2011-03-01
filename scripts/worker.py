from gevent import wsgi

def outputter(env, start):
    print env
    start('200 OK', {})
    return ["ok"]

print 'Serving on 8080...'
wsgi.WSGIServer(('', 8080), outputter).serve_forever()