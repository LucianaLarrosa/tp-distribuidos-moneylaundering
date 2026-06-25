import logging
import threading

from common.communication.protocol import health
from common.communication.socket import SafeUDPSocket, SocketTimeoutError


class HealthResponder:
    _POLL_TIMEOUT = 1.0

    def __init__(self, node_name, ping_port, host):
        self._pong_payload = health.serialize_pong(node_name)
        self._socket = SafeUDPSocket()
        self._socket.bind(host, ping_port)
        self._stop_event = threading.Event()
        self._stop_event.set()
        self._thread = None

    def _run(self):
        """
        Listen for incoming ping messages and respond with a pong.
        """
        while not self._stop_event.is_set():
            try:
                data, source_address = self._socket.recv(timeout=self._POLL_TIMEOUT)
            except SocketTimeoutError:
                continue
            except OSError:
                break
            try:
                health.deserialize_ping(data)
            except Exception as e:
                logging.warning("Ignoring malformed ping: %s", e)
                continue
            try:
                self._socket.send(self._pong_payload, source_address)
            except OSError as e:
                logging.warning("Failed to send pong: %s", e)

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join()
        self._thread = None

    def close(self):
        self.stop()
        self._socket.close()
