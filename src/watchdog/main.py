import logging
import signal
import subprocess
import threading
import time

from common.protocol.health import health
from common.socket import SafeUDPSocket
from config import Config


class Watchdog:
    WATCHDOG_HOST = "0.0.0.0"

    def __init__(self, config):
        self._config = config
        self._closed = False
        self._last_seen = {}  # node_name -> last heartbeat timestamp
        self._lock = threading.Lock()
        self._socket = SafeUDPSocket()
        self._socket.bind(self.WATCHDOG_HOST, config.port)
        self._receiver_thread = threading.Thread(target=self._receive_loop, daemon=True)

        signal.signal(signal.SIGTERM, lambda *_: self.shutdown())

    def _update_last_seen(self, node_name):
        with self._lock:
            self._last_seen[node_name] = time.monotonic()

    def _receive_loop(self):
        """
        Continuously receive heartbeat messages from nodes and update their last seen timestamps.
        """
        while not self._closed:
            try:
                data, _ = self._socket.recv()
            except OSError:
                break
            try:
                node_name = health.deserialize_heartbeat(data)
            except Exception as e:
                logging.warning("Ignoring malformed heartbeat: %s", e)
                continue
            if not node_name:
                continue
            self._update_last_seen(node_name)

    def _revive(self, node_name):
        """
        Attempt to revive a node by starting its Docker container.
        """
        result = subprocess.run(
            ["docker", "start", node_name], capture_output=True, text=True
        )
        if result.returncode != 0:
            logging.error("Failed to revive '%s': %s", node_name, result.stderr.strip())
        else:
            logging.info("Node '%s' revived", node_name)

    def _get_dead_nodes(self):
        now = time.monotonic()
        with self._lock:
            return [
                node
                for node, last_seen in self._last_seen.items()
                if now - last_seen > self._config.timeout_seconds
            ]

    def _check_once(self):
        """
        Check for nodes that have not sent a heartbeat within the timeout period and attempt to revive them.
        """
        for node in self._get_dead_nodes():
            logging.warning(
                "Node %s timed out (no heartbeat for %d seconds), attempting to revive",
                node,
                self._config.timeout_seconds,
            )
            self._revive(node)
            self._update_last_seen(node)

    def start(self):
        logging.info("Starting watchdog...")
        self._receiver_thread.start()
        while not self._closed:
            self._check_once()
            time.sleep(self._config.check_interval_seconds)

    def shutdown(self):
        if self._closed:
            return
        self._closed = True
        logging.info("Shutting down watchdog...")
        self._socket.close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [Watchdog] %(levelname)s %(message)s",
    )
    config = Config()
    watchdog = Watchdog(config)
    try:
        watchdog.start()
    except Exception as e:
        logging.error("Error during Watchdog execution: %s", e)
    finally:
        watchdog.shutdown()


if __name__ == "__main__":
    main()
