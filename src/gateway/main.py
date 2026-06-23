import logging
import multiprocessing
import signal

from gateway.config import Config
from gateway.internal.client_handler import ClientHandler
from gateway.internal.client_registry import ClientRegistry
from gateway.internal.internal_router import InternalRouter
from gateway.internal.reaper import Reaper
from common.health import HealthResponder
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.protocol.external import external
from common.protocol.external.external import MsgType
from common.socket.safe_socket import SafeTCPSocket


def _handle_client_process(sock, client_id, config, registry):
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

    try:
        results = MessageMiddlewareExchangeDirectRabbitMQ(
            config.rabbitmq_host,
            config.query_results_exchange,
            [client_id],
            queue_name=f"{config.results_queue_prefix}.{client_id}",
        )
    except Exception:
        logging.exception("[%s] failed to connect to results queue", client_id)
        exchange.close()
        sock.close()
        raise

    router = InternalRouter(
        exchange, config.transaction_routing_key, config.account_routing_key
    )
    try:
        ClientHandler(sock, client_id, router, results, registry).run()
    except Exception:
        logging.exception("[%s] handler crashed", client_id)
        raise
    finally:
        exchange.close()


def _worker_init():
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


class Gateway:
    def __init__(self, config):
        self._config = config
        self._server_sock = SafeTCPSocket()
        self._server_sock.bind(config.listen_host, config.listen_port)
        self._server_sock.listen()
        self._pool = multiprocessing.Pool(
            processes=config.pool_size, initializer=_worker_init
        )
        self._manager = multiprocessing.Manager()
        self._registry = ClientRegistry(self._manager, config.state_dir)
        self._health_responder = HealthResponder(
            config.node_name, config.ping_port, config.ping_pong_host
        )
        self._reaper = None
        self._closed = False

    def run(self):
        logging.info(
            "Gateway listening on port %s (pool size: %s)",
            self._config.listen_port,
            self._config.pool_size,
        )
        self._health_responder.start()
        self._registry.load()
        self._start_reaper()

        try:
            while True:
                client_sock, addr = self._server_sock.accept()
                msg_type, client_id = external.recv_msg(client_sock)
                if msg_type != MsgType.ANNOUNCE:
                    logging.error(
                        "Expected ANNOUNCE, got msg_type=%s, closing", msg_type
                    )
                    client_sock.close()
                    continue
                logging.info("Client %s connected from %s", client_id, addr)

                self._pool.apply_async(
                    _handle_client_process,
                    args=(
                        client_sock,
                        client_id,
                        self._config,
                        self._registry,
                    ),
                    error_callback=self._on_client_error,
                )
        except OSError:
            if not self._closed:
                raise

    def _start_reaper(self):
        self._reaper = Reaper(self._registry, self._config)
        self._reaper.start()

    def _on_client_error(self, error):
        logging.error("client handler process error: %s", error)

    def shutdown(self, signum=None, frame=None):
        if self._closed:
            return
        self._closed = True
        logging.info("Shutdown requested")
        self._health_responder.stop()
        if self._reaper is not None:
            self._reaper.stop()
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
