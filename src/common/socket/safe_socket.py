import socket
from typing import Optional


class SafeSocket:
    def __init__(self, sock: socket.socket) -> None:
        pass

    @classmethod
    def connect(cls, host: str, port: int) -> SafeSocket:
        pass

    def send(self, data: bytes) -> None:
        pass

    def recv(self) -> bytes:
        pass

    def close(self) -> None:
        pass

    def _send_all(self, data: bytes) -> None:
        pass

    def _recv_exact(self, n: int) -> Optional[bytes]:
        pass
