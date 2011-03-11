
def line_protocol(socket, strip=True):
    fileobj = socket.makefile()
    while True:
        try:
            line = fileobj.readline() # returns None on EOF
            if line is not None and strip:
                line = line.strip()
        except IOError:
            line = None
        if line is not None:
            yield line
        else:
            break