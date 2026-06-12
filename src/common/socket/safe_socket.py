import socket
from abc import ABC, abstractmethod


class SocketTimeoutError(Exception):
    pass


class IncompleteReadError(Exception):
    def __init__(self, partial: bytes, expected: int):
        self.partial = partial
        self.expected = expected
        super().__init__(partial, expected)

    def __str__(self):
        return f"Expected {self.expected} bytes, got {len(self.partial)}"

    def __reduce__(self):
        return (self.__class__, (self.partial, self.expected))


class BaseSafeSocket(ABC):
    def __init__(self, sock):
        self._sock = sock

    def bind(self, host, port):
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((host, port))

    @abstractmethod
    def send(self, *args):
        pass

    @abstractmethod
    def recv(self, *args):
        pass

    @abstractmethod
    def close(self):
        pass


class SafeTCPSocket(BaseSafeSocket):
    def __init__(self, sock=None):
        super().__init__(sock or socket.socket(socket.AF_INET, socket.SOCK_STREAM))

    def connect(self, host, port):
        self._sock.connect((host, port))

    def listen(self):
        self._sock.listen()

    def accept(self):
        conn, address = self._sock.accept()
        return SafeTCPSocket(conn), address

    def send(self, data):
        self._sock.sendall(data)

    def recv(self, size):
        buf = bytearray(size)
        pos = 0
        while pos < size:
            n = self._sock.recv_into(memoryview(buf)[pos:])
            if n == 0:
                raise IncompleteReadError(bytes(buf[:pos]), size)
            pos += n
        return bytes(buf)

    def close(self):
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()


class SafeUDPSocket(BaseSafeSocket):
    BUF_SIZE = 1024

    def __init__(self, sock=None):
        super().__init__(sock or socket.socket(socket.AF_INET, socket.SOCK_DGRAM))

    def send(self, data, address):
        self._sock.sendto(data, address)

    def recv(self, timeout=None):
        self._sock.settimeout(timeout)
        try:
            return self._sock.recvfrom(self.BUF_SIZE)
        except socket.timeout:
            raise SocketTimeoutError()

    def close(self):
        self._sock.close()
