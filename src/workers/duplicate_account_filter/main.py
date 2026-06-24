import logging

from common.communication.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.idempotency.ids import eof_id, final_eof_id
from common.models.query_results import Q4Result
from common.communication.protocol import internal
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from config import Config


class DuplicateAccountFilter(StatefulCoordinatedWorker):
    def __init__(self, config):
        super().__init__(config)
        self._unique_accounts = (
            {}
        )  # (client_id) -> {(bank, account) -> Q4Result}

        self._input_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            routing_keys=[self._get_ring_routing_key(config.node_id)],
            queue_name=f"{config.input_exchange}_{config.node_id}",
        )
        self._output_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
            routing_keys=[],
        )

    @property
    def _input_middleware(self):
        return self._input_exchange

    @property
    def _output_middleware(self):
        return self._output_exchange

    @property
    def _rabbitmq_host(self):
        return self.config.rabbitmq_host

    @property
    def _control_exchange_name(self):
        return self.config.control_exchange

    @property
    def _node_prefix(self):
        return self.config.node_prefix

    @property
    def _node_id(self):
        return self.config.node_id

    @property
    def _ring_size(self):
        return self.config.ring_size

    def _flush_data(self, client_id):
        unique_accounts = self._unique_accounts.pop(client_id, {})
        self._flush_sharded(
            self._output_exchange,
            internal.MsgType.Q4_RESULT_BATCH,
            client_id,
            list(unique_accounts.values()),
            key_of=lambda result: f"{result.bank}.{result.account}",
            num_shards=1,
            batch_size=self.config.batch_size,
            routing_key_for=lambda _shard: client_id,
        )

    def _send_final_eof(self, client_id, eof):
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.QUERY_END,
                client_id,
                self.config.query_id,
                eof.message_count,
                message_id=final_eof_id(client_id, eof, self.config.query_id),
            ),
            routing_key=client_id,
        )

    def _handle_data_message(self, _, client_id, payload):
        """
        Handle incoming data messages by storing unique (bank, account) pairs for each client and gateway.
        """
        super()._handle_data_message(_, client_id, payload)
        delta = [[account.bank, account.account] for account in payload]
        self._apply_delta(client_id, delta)
        return delta

    def _cleanup_state(self, client_id):
        super()._cleanup_state(client_id)
        self._unique_accounts.pop(client_id, None)

    def _apply_delta(self, client_id, delta):
        unique_accounts = self._unique_accounts.setdefault(client_id, {})
        for bank, account in delta:
            unique_accounts[(bank, account)] = Q4Result(bank, account)

    def _state_as_delta(self, client_id):
        return [
            [result.bank, result.account]
            for result in self._unique_accounts.get(
                (client_id), {}
            ).values()
        ]


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [DuplicateAccountFilter] %(levelname)s %(message)s",
    )
    config = Config()
    worker = DuplicateAccountFilter(config)
    try:
        worker.start()
    except Exception as e:
        logging.error(f"Error during DuplicateAccountFilter execution: {e}")
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
