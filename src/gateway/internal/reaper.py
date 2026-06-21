import logging
import threading

from gateway.internal.internal_router import InternalRouter
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)


class Reaper:
    def __init__(self, registry, config):
        self._registry = registry
        self._config = config
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join()

    def _run(self):
        exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            self._config.rabbitmq_host,
            self._config.raw_data_exchange,
            [],
        )
        router = InternalRouter(
            exchange,
            self._config.transaction_routing_key,
            self._config.account_routing_key,
        )
        try:
            while not self._stop.wait(self._config.reaper_interval):
                try:
                    self._registry.persist()
                    for client_id, gateway_id in self._registry.dead_clients(
                        self._config.reaper_timeout
                    ):
                        logging.info("[reaper] reaping dead client %s", client_id)
                        router.forward_cleanup_eof(client_id, gateway_id)
                        self._registry.remove(client_id)
                except Exception:
                    logging.exception("[reaper] error during reap cycle")
        finally:
            exchange.close()
