import logging
import threading

from common.protocol.health import health
from common.socket import SafeUDPSocket


class HeartbeatSender:
    def __init__(self, node_name, watchdog_host, watchdog_port, interval_seconds):
        self._address = (watchdog_host, watchdog_port)
        self._payload = health.serialize_heartbeat(node_name)
        self._interval_seconds = interval_seconds
        self._socket = SafeUDPSocket()
        self._stop_event = threading.Event()
        self._heartbeat_thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        """
        Periodically send a heartbeat message to the watchdog until stopped.
        """
        while not self._stop_event.is_set():
            try:
                self._socket.send(self._payload, self._address)
            except OSError as e:
                logging.warning("Failed to send heartbeat: %s", e)
            self._stop_event.wait(self._interval_seconds)

    def start(self):
        self._heartbeat_thread.start()

    def stop(self):
        self._stop_event.set()
        if self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join()
        self._socket.close()
