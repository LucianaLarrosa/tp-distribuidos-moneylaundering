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
from common.protocol.internal import internal
from common.socket.safe_socket import SafeSocket


def _handle_client_process(sock, client_id, gateway_id, config, results_queue):
    try:
        exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            config.rabbitmq_host,
            config.raw_data_exchange,
            [],
        )
    except Exception:
        logging.exception("[%s] failed to connect to RabbitMQ exchange", client_id)
        sock.close()
        raise

    router = InternalRouter(
        exchange, config.transaction_routing_key, config.account_routing_key
    )
    try:
        ClientHandler(sock, client_id, gateway_id, router, results_queue).run()
    except Exception:
        logging.exception("[%s] handler crashed", client_id)
        raise
    finally:
        exchange.close()


def _worker_init():
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


def _run_results_consumer(rabbitmq_host, exchange_name, gateway_id, client_queues):
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    middleware = MessageMiddlewareExchangeDirectRabbitMQ(
        rabbitmq_host, exchange_name, [gateway_id]
    )

    def on_message(body, ack, _nack):
        msg_type, client_id, _, payload, _ = internal.deserialize_msg(body)
        handler_queue = client_queues.get(client_id)
        if handler_queue is None:
            logging.warning(
                "[results_consumer] no handler queue for client_id=%s, dropping %s",
                client_id,
                msg_type,
            )
            ack()
            return
        handler_queue.put((msg_type, payload))
        ack()

    try:
        middleware.start_consuming(on_message)
    finally:
        middleware.close()


class Gateway:
    def __init__(self, config):
        self._config = config
        self._gateway_id = str(uuid.uuid4())
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.bind((config.listen_host, config.listen_port))
        self._server_sock.listen()
        self._pool = multiprocessing.Pool(
            processes=config.pool_size, initializer=_worker_init
        )
        self._manager = multiprocessing.Manager()
        self._client_queues = self._manager.dict()
        self._results_consumer = multiprocessing.Process(
            target=_run_results_consumer,
            args=(
                config.rabbitmq_host,
                config.query_results_exchange,
                self._gateway_id,
                self._client_queues,
            ),
            daemon=True,
        )
        self._closed = False

    def run(self):
        logging.info(
            "Gateway %s listening on port %s (pool size: %s)",
            self._gateway_id,
            self._config.listen_port,
            self._config.pool_size,
        )
        self._results_consumer.start()

        try:
            while True:
                client_sock_raw, addr = self._server_sock.accept()
                client_sock = SafeSocket(client_sock_raw)
                client_id = str(uuid.uuid4())
                results_queue = self._manager.Queue()
                self._client_queues[client_id] = results_queue
                logging.info("Client %s connected from %s", client_id, addr)

                self._pool.apply_async(
                    _handle_client_process,
                    args=(
                        client_sock,
                        client_id,
                        self._gateway_id,
                        self._config,
                        results_queue,
                    ),
                    callback=self._make_client_cleanup(client_id),
                    error_callback=self._make_client_cleanup(client_id),
                )
        except OSError:
            if not self._closed:
                raise

    def _make_client_cleanup(self, client_id):
        def _cleanup(_result_or_error):
            self._client_queues.pop(client_id, None)

        return _cleanup

    def shutdown(self, signum=None, frame=None):
        if self._closed:
            return
        self._closed = True
        logging.info("Shutdown requested")
        self._server_sock.close()
        if self._results_consumer.is_alive():
            self._results_consumer.terminate()
            self._results_consumer.join()
        self._pool.terminate()
        self._pool.join()
        self._manager.shutdown()


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
