import logging

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareExchangeTopicRabbitMQ,
)
from common.protocol.internal import internal
from common.sharding import shard_of
from common.worker.stateless_worker import StatelessWorker
from config import Config

PAYMENT_FORMAT_EOF_SHARD = "0"


class DateFilter(StatelessWorker):
    def __init__(self, config):
        super().__init__()
        self.config = config

        self._input_exchange = MessageMiddlewareExchangeTopicRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            binding_patterns=config.input_routing_keys,
            queue_name=config.input_queue_name,
        )
        self._output_exchange = MessageMiddlewareExchangeTopicRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
            binding_patterns=[],
        )
        self._payment_format_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.payment_format_exchange,
            routing_keys=[],
        )

    @property
    def _input_middleware(self):
        return self._input_exchange

    @property
    def _output_middleware(self):
        return self._output_exchange

    def _send_final_eof(self, client_id, gateway_id, eof):
        msg = internal.serialize_msg(internal.MsgType.EOF, client_id, gateway_id, eof)
        self._output_exchange.send(msg, routing_key=self.config.output_routing_key_eof)
        self._payment_format_exchange.send(msg, routing_key=PAYMENT_FORMAT_EOF_SHARD)

    def shutdown(self):
        super().shutdown()
        self._payment_format_exchange.close()

    def _classify_transaction(self, transaction):
        """
        Classify a transaction based on its timestamp and currency, returning the appropriate routing key for the output exchange.
        """
        transaction_timestamp = transaction.timestamp
        is_usd = transaction.currency.lower() == self.config.usd_currency
        if self.config.date_from_1 <= transaction_timestamp <= self.config.date_to_1:
            routing_keys = [
                f"{self.config.output_routing_key_all}.{self.config.output_routing_key_period_1}"
            ]
            if is_usd:
                routing_keys.append(
                    f"{self.config.output_routing_key_usd}.{self.config.output_routing_key_period_1}"
                )
            return routing_keys
        if (
            self.config.date_from_2 <= transaction_timestamp <= self.config.date_to_2
            and is_usd
        ):
            return [
                f"{self.config.output_routing_key_usd}.{self.config.output_routing_key_period_2}"
            ]
        return []

    def _handle_data_message(self, _, client_id, gateway_id, payload):
        """
        Handle incoming data messages by classifying transactions and sending them to the output exchange with the appropriate routing keys.
        """
        transactions_by_routing_key = {
            routing_key: []
            for routing_key in [
                f"{self.config.output_routing_key_usd}.{self.config.output_routing_key_period_1}",
                f"{self.config.output_routing_key_usd}.{self.config.output_routing_key_period_2}",
                f"{self.config.output_routing_key_all}.{self.config.output_routing_key_period_1}",
            ]
        }
        for transaction in payload:
            for routing_key in self._classify_transaction(transaction):
                transactions_by_routing_key[routing_key].append(transaction)
        for routing_key, transactions in transactions_by_routing_key.items():
            self._send(
                self._output_exchange,
                internal.MsgType.TRANSACTION_BATCH,
                client_id,
                gateway_id,
                transactions,
                routing_key=routing_key,
            )
        usd_period_1_key = (
            f"{self.config.output_routing_key_usd}."
            f"{self.config.output_routing_key_period_1}"
        )
        self._send(
            self._payment_format_exchange,
            internal.MsgType.TRANSACTION_BATCH,
            client_id,
            gateway_id,
            transactions_by_routing_key[usd_period_1_key],
            routing_key=str(
                shard_of(
                    self._current_message_id, self.config.payment_format_node_count
                )
            ),
        )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [DateFilter] %(levelname)s %(message)s",
    )
    config = Config()
    date_filter = DateFilter(config)
    try:
        date_filter.start()
    except Exception as e:
        logging.error(f"Error during DateFilter execution: {e}")
    finally:
        date_filter.shutdown()


if __name__ == "__main__":
    main()
