import socket


class IncompleteReadError(Exception):
    def __init__(self, partial: bytes, expected: int):
        self.partial = partial
        self.expected = expected
        super().__init__(f"Expected {expected} bytes, got {len(partial)}")


class SafeSocket:
    def __init__(self, sock):
        self._sock = sock

    @classmethod
    def connect(cls, host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        return cls(sock)

    def close(self):
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()

    def send_all(self, data):
        self._sock.sendall(data)

    def recv_exact(self, size):
        buf = bytearray(size)
        pos = 0
        while pos < size:
            n = self._sock.recv_into(memoryview(buf)[pos:])
            if n == 0:
                raise IncompleteReadError(bytes(buf[:pos]), size)
            pos += n
        return bytes(buf)
