import logging
import multiprocessing
import signal
import socket

from gateway.config import Config
from gateway.internal.client_handler import handle_client
from common.socket.safe_socket import SafeSocket


class Gateway:
    def __init__(self, config):
        self._config = config
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.bind((config.listen_host, config.listen_port))
        self._server_sock.listen()
        self._pool = multiprocessing.Pool(processes=config.pool_size)
        self._closed = False

    def run(self):
        logging.info(f"Gateway listening on port {self._config.listen_port} (pool size: {self._config.pool_size})")

        try:
            while True:
                client_sock_raw, addr = self._server_sock.accept()
                client_sock = SafeSocket(client_sock_raw)
                logging.info(f"Client connected from {addr}")

                self._pool.apply_async(
                    handle_client,
                    args=(client_sock, self._config.debug_output_dir),
                )
        except OSError:
            if not self._closed:
                raise

    def shutdown(self, signum=None, frame=None):
        if self._closed:
            return
        self._closed = True
        logging.info("Shutdown requested")
        self._server_sock.close()
        self._pool.terminate()
        self._pool.join()


def main():
    multiprocessing.set_start_method("fork")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = Config.from_env()
    gateway = Gateway(config)
    signal.signal(signal.SIGTERM, gateway.shutdown)
    try:
        gateway.run()
    finally:
        gateway.shutdown()


if __name__ == "__main__":
    main()
