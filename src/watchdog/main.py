import logging
import signal
import subprocess
import time

from common.protocol.health import health
from common.socket import SafeUDPSocket, SocketTimeoutError
from config import Config


class Watchdog:
    def __init__(self, config):
        self._config = config
        self._closed = False
        self._seen = set()
        self._miss_count = (
            {}
        )  # node -> consecutive missed pongs (only tracked after first pong)
        self._socket = SafeUDPSocket()
        self._socket.bind(config.ping_pong_host, config.pong_port)

        signal.signal(signal.SIGTERM, lambda *_: self.shutdown())

    def _send_pings(self):
        ping_data = health.serialize_ping()
        for node in self._config.monitored_nodes:
            try:
                self._socket.send(ping_data, (node, self._config.ping_port))
            except OSError as e:
                logging.warning("Failed to ping '%s': %s", node, e)

    def _collect_pongs(self):
        """
        Collect pong responses from nodes until the timeout expires, returning a set of nodes that responded.
        """
        responded = set()
        deadline = time.monotonic() + self._config.ping_timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                data, _ = self._socket.recv(timeout=remaining)
            except (SocketTimeoutError, OSError):
                break
            try:
                node_name = health.deserialize_pong(data)
                if node_name:
                    responded.add(node_name)
            except Exception as e:
                logging.warning("Ignoring malformed pong: %s", e)
        return responded

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

    def _process_results(self, responded):
        """
        Update internal state based on which nodes responded to the ping, and attempt to revive any that have missed too many pongs.
        """
        self._seen.update(responded)
        for node in responded:
            self._miss_count[node] = 0
        for node in self._seen - responded:
            self._miss_count[node] += 1
            if self._miss_count[node] >= self._config.max_retries:
                logging.warning(
                    "Node '%s' presumed dead after %d missed pongs, reviving...",
                    node,
                    self._miss_count[node],
                )
                self._revive(node)
                self._miss_count[node] = 0

    def start(self):
        logging.info("Starting watchdog...")
        while not self._closed:
            self._send_pings()
            responded = self._collect_pongs()
            self._process_results(responded)
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
