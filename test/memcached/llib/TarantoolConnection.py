import errno
import socket
import struct
import cStringIO

class TarantoolConnection(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.is_connected = False
        self.stream = cStringIO.StringIO()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)

    def connect(self):
        self.socket.connect((self.host, self.port))
        self.is_connected = True

    def disconnect(self):
        if self.is_connected:
            self.socket.close()
            self.is_connected = False

    def reconnect(self):
        self.disconnect()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self.connect()

    def opt_reconnect(self):
        """ On a socket which was disconnected, recv of 0 bytes immediately
            returns with no data. On a socket which is alive, it returns EAGAIN.
            Make use of this property and detect whether or not the socket is
            dead. Reconnect a dead socket, do nothing if the socket is good."""
        try:
            if self.socket.recv(0, socket.MSG_DONTWAIT) == '':
                self.reconnect()
        except socket.error as e:
            if e.errno == errno.EAGAIN:
                pass
            else:
                self.reconnect()

    def execute(self, command, silent = False):
        self.opt_reconnect()
        return self.execute_no_reconnect(command, silent)

    def __call__(self, command, silent = False):
        return self.execute(command, silent)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, tb):
        self.disconnect()


