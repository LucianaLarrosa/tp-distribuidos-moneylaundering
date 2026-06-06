import logging

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.models.query_results import Q4Result
from common.protocol.internal import internal
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from config import Config


class DuplicateAccountFilter(StatefulCoordinatedWorker):
    def __init__(self, config):
        self.config = config
        super().__init__()
        self._unique_accounts = (
            {}
        )  # (client_id, gateway_id) -> {(bank, account) -> Q4Result}

        self._input_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            routing_keys=[self._get_ring_routing_key(config.node_id)],
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

    def _flush_data_in_batches(self, client_id, gateway_id, result):
        """
        Flush the given result in batches with the specified routing key.
        """
        for i in range(0, len(result), self.config.batch_size):
            self._output_exchange.send(
                internal.serialize_msg(
                    internal.MsgType.Q4_RESULT_BATCH,
                    client_id,
                    gateway_id,
                    result[i : i + self.config.batch_size],
                ),
                routing_key=gateway_id,
            )
            self._increment_sent_count(client_id, gateway_id)

    def _flush_data(self, client_id, gateway_id):
        unique_accounts = self._unique_accounts.pop((client_id, gateway_id), {})
        self._flush_data_in_batches(
            client_id,
            gateway_id,
            list(unique_accounts.values()),
        )

    def _send_final_eof(self, client_id, gateway_id, eof):
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.QUERY_END,
                client_id,
                gateway_id,
                self.config.query_id,
                eof.message_count,
            ),
            routing_key=gateway_id,
        )

    def _handle_data_message(self, _, client_id, gateway_id, payload):
        """
        Handle incoming data messages by storing unique (bank, account) pairs for each client and gateway.
        """
        unique_accounts = self._unique_accounts.setdefault((client_id, gateway_id), {})
        for account in payload:
            unique_accounts[(account.bank, account.account)] = account
        super()._handle_data_message(_, client_id, gateway_id, payload)


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
