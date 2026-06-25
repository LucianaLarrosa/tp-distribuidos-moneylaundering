import logging
import subprocess
import threading
import time

from common.communication.protocol import health
from common.communication.socket import SafeUDPSocket, SocketTimeoutError


class HealthMonitor:
    def __init__(
        self,
        monitored_hosts,
        host,
        pong_port,
        ping_port,
        ping_timeout,
        check_interval,
        max_retries,
    ):
        self._monitored_hosts = monitored_hosts
        self._udp_socket = SafeUDPSocket()
        self._udp_socket.bind(host, pong_port)
        self._ping_port = ping_port
        self._ping_timeout = ping_timeout
        self._check_interval = check_interval
        self._max_retries = max_retries
        self._miss_count = {}
        self._stop_event = threading.Event()
        self._stop_event.set()
        self._thread = None

    def _send_pings(self):
        for node in self._monitored_hosts:
            try:
                self._udp_socket.send(health.serialize_ping(), (node, self._ping_port))
            except OSError as e:
                logging.warning("Failed to ping '%s': %s", node, e)

    def _collect_pongs(self):
        """
        Collect pongs until the ping timeout expires, returning the set of nodes that responded.
        """
        responded = set()
        deadline = time.monotonic() + self._ping_timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                data, _ = self._udp_socket.recv(timeout=remaining)
            except SocketTimeoutError:
                break
            except OSError:
                if self._stop_event.is_set():
                    break
                continue
            try:
                node_name = health.deserialize_pong(data)
                if node_name:
                    responded.add(node_name)
            except Exception as e:
                logging.warning("Ignoring malformed pong: %s", e)
        return responded

    def _revive(self, node_name):
        result = subprocess.run(
            ["docker", "start", node_name], capture_output=True, text=True
        )
        if result.returncode != 0:
            logging.error("Failed to revive '%s': %s", node_name, result.stderr.strip())
        else:
            logging.info("Node '%s' revived", node_name)

    def _process_results(self, responded):
        """
        Update miss counts based on which nodes responded, and revive any that exceeded the max retries threshold.
        """
        for node in self._monitored_hosts:
            if node in responded:
                self._miss_count[node] = 0
            else:
                self._miss_count[node] = self._miss_count.get(node, 0) + 1
                if self._miss_count[node] >= self._max_retries:
                    logging.warning(
                        "Node '%s' presumed dead after %d missed pongs, reviving...",
                        node,
                        self._miss_count[node],
                    )
                    self._revive(node)
                    self._miss_count[node] = 0

    def _run(self):
        while not self._stop_event.is_set():
            self._send_pings()
            responded = self._collect_pongs()
            self._process_results(responded)
            self._stop_event.wait(self._check_interval)

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()

    def close(self):
        self._stop_event.set()
        self._udp_socket.close()
