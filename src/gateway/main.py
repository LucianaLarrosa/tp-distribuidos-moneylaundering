import logging
import multiprocessing
import signal
import socket
import uuid

from gateway.config import Config
from gateway.internal.client_handler import ClientHandler
from gateway.internal.internal_router import InternalRouter
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.socket.safe_socket import SafeSocket


def _handle_client_process(sock, gateway_id, config):
    exchange = MessageMiddlewareExchangeDirectRabbitMQ(
        config.rabbitmq_host,
        config.raw_data_exchange,
        [config.transaction_routing_key, config.account_routing_key],
    )
    router = InternalRouter(
        exchange, config.transaction_routing_key, config.account_routing_key
    )
    try:
        ClientHandler(sock, gateway_id, router).run()
    finally:
        exchange.close()


class Gateway:
    def __init__(self, config):
        self._config = config
        self._gateway_id = str(uuid.uuid4())
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.bind((config.listen_host, config.listen_port))
        self._server_sock.listen()
        self._pool = multiprocessing.Pool(processes=config.pool_size)
        self._closed = False

    def run(self):
        logging.info(
            f"Gateway {self._gateway_id} listening on port {self._config.listen_port} "
            f"(pool size: {self._config.pool_size})"
        )

        try:
            while True:
                client_sock_raw, addr = self._server_sock.accept()
                client_sock = SafeSocket(client_sock_raw)
                logging.info(f"Client connected from {addr}")

                self._pool.apply_async(
                    _handle_client_process,
                    args=(client_sock, self._gateway_id, self._config),
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
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    config = Config.from_env()
    gateway = Gateway(config)
    signal.signal(signal.SIGTERM, gateway.shutdown)
    try:
        gateway.run()
    finally:
        gateway.shutdown()


if __name__ == "__main__":
    main()
