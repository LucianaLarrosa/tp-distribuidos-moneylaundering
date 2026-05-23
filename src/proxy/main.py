import logging
import signal
import socket
import threading

from proxy.config import Config
from common.socket.safe_socket import SafeSocket
from common.protocol import external
from common.protocol.external import MsgType


class Proxy:
    def __init__(self, config):
        self._config = config
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.bind((config.listen_host, config.listen_port))
        self._server_sock.listen()
        self._next_index = 0
        self._index_lock = threading.Lock()
        self._closed = False

    def run(self):
        logging.info(
            "Proxy listening on %s:%s (gateways: %s, port %s)",
            self._config.listen_host or "0.0.0.0",
            self._config.listen_port,
            self._config.gateway_hosts,
            self._config.gateway_port,
        )
        try:
            while True:
                client_sock_raw, addr = self._server_sock.accept()
                threading.Thread(
                    target=self._handle_client,
                    args=(client_sock_raw, addr),
                    daemon=True,
                ).start()
        except OSError:
            if not self._closed:
                raise

    def _pick_gateway(self):
        with self._index_lock:
            host = self._config.gateway_hosts[self._next_index]
            self._next_index = (self._next_index + 1) % len(self._config.gateway_hosts)
        return host

    def _handle_client(self, client_sock_raw, addr):
        sock = SafeSocket(client_sock_raw)
        try:
            host = self._pick_gateway()
            external.send_msg(sock, MsgType.REDIRECT, host, self._config.gateway_port)
        except Exception:
            logging.exception("Failed to redirect client %s", addr)
        finally:
            sock.close()

    def shutdown(self, signum=None, frame=None):
        if self._closed:
            return
        self._closed = True
        logging.info("Shutdown requested")
        self._server_sock.close()


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    config = Config.from_env()
    proxy = Proxy(config)
    signal.signal(signal.SIGTERM, proxy.shutdown)
    try:
        proxy.run()
    finally:
        proxy.shutdown()


if __name__ == "__main__":
    main()
