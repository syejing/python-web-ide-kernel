import sys
import zmq
from session import extract_header


class InStream(object):
    def __init__(self, session, socket, name):
        self.session = session
        self.socket = socket
        self.name = name
        self.parent_header = {}
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    def set_parent(self, parent):
        self.parent_header = extract_header(parent)

    def isattr(self):
        return False

    def next(self):
        pass

    def read(self, size=None):
        msg = self.session.msg("input_request", parent=self.parent_header)
        self.socket.send_json(msg)
        retries_left = 1000
        while True:
            try:
                socks = dict(self.poller.poll(50))
                if self.socket in socks and socks[self.socket] == zmq.POLLIN:
                    reply = self.socket.recv_json(zmq.DONTWAIT)
                    print(reply, file=sys.__stdout__)
                    return str(reply["content"]["data"])
                else:
                    retries_left -= 1
                    if retries_left == 0:
                        return ""
            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    pass
                else:
                    raise

    def readline(self, size=None):
        if self.socket is None:
            raise ValueError("I/O operation on closed file")
        return self.read(size)

    def write(self, s):
        raise IOError("Write not supported on a write only stream.")

    writelines = write

    def close(self):
        self.socket = None

    def _maybe_send(self):
        raise IOError("Write not supported on a write only stream.")

    def flush(self):
        raise IOError("Write not supported on a write only stream.")


class OutStream(object):
    def __init__(self, session, pub_socket, name, max_buffer=200):
        self.session = session
        self.pub_socket = pub_socket
        self.name = name
        self.parent_header = {}
        self._buffer = []
        self._buffer_len = 0
        self.max_buffer = max_buffer

    def set_parent(self, parent):
        self.parent_header = extract_header(parent)

    def isattr(self):
        return False

    def next(self):
        raise IOError("Read not supported on a write only stream.")

    def read(self, size=None):
        raise IOError("Read not supported on a write only stream.")

    readline = read

    def write(self, s):
        if self.pub_socket is None:
            raise ValueError("I/O operation on closed file")
        else:
            self._buffer.append(s)
            self._buffer_len += len(s)
            self._maybe_send()

    def writelines(self, sequence):
        if self.pub_socket is None:
            raise ValueError("I/O operation on closed file")
        else:
            for s in sequence:
                self.write(s)

    def close(self):
        self.pub_socket = None

    def _maybe_send(self):
        if "\n" in self._buffer[-1]:
            self.flush()
        if self._buffer_len > self.max_buffer:
            self.flush()

    def flush(self):
        if self.pub_socket is None:
            raise ValueError("I/O operation on closed file")
        else:
            if self._buffer:
                data = "".join(self._buffer)
                content = {"name": self.name, "data": data}
                msg = self.session.msg("stream", content=content, parent=self.parent_header)
                self.pub_socket.send_json(msg)
                self._buffer_len = 0
                self._buffer = []
