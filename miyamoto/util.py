
def line_protocol(socket, strip=True):
    fileobj = socket.makefile()
    while True:
        try:
            line = fileobj.readline()
            if strip:
                line = line.strip()
        except IOError:
            line = None
        if line:
            yield line
        else:
            break