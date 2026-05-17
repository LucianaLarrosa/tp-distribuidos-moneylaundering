import logging
from datetime import datetime

from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeTopicRabbitMQ
from common.protocol import internal
from common.worker.stateless_worker import StatelessWorker
from config import Config


class DateFilter(StatelessWorker):
    def __init__(self, config):
        """
        Initialize the DateFilter worker with the given configuration.
        """
        super().__init__()
        self.config = config

        self._input_exchange = MessageMiddlewareExchangeTopicRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            binding_patterns=[config.input_routing_key],
        )
        self._output_exchange = MessageMiddlewareExchangeTopicRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
            binding_patterns=[],
        )

    @property
    def _input_middleware(self):
        """
        Return the input exchange to consume messages from the previous stage.
        """
        return self._input_exchange

    @property
    def _output_middleware(self):
        """
        Return the output exchange to forward messages to the next stage.
        """
        return self._output_exchange

    def _send_final_eof(self, client_id, gateway_id, eof):
        """
        Send the final EOF message to the next stage's output exchange with the appropriate routing key.
        """
        self._output_exchange.send(
            internal.serialize_msg(internal.MsgType.EOF, client_id, gateway_id, eof),
            routing_key=self.config.output_routing_key_eof,
        )

    def _classify_transaction(self, transaction):
        """
        Classify a transaction based on its timestamp and currency, returning the appropriate routing key for the output exchange.
        """
        transaction_timestamp = datetime.strptime(
            transaction.timestamp, self.config.date_format
        )
        currency_key = (
            self.config.output_routing_key_usd
            if transaction.currency.lower() == self.config.usd_currency
            else self.config.output_routing_key_no_usd
        )
        if self.config.date_from_1 <= transaction_timestamp <= self.config.date_to_1:
            return f"{currency_key}.{self.config.output_routing_key_period_1}"
        if (
            self.config.date_from_2 <= transaction_timestamp <= self.config.date_to_2
            and currency_key == self.config.output_routing_key_usd
        ):
            return f"{currency_key}.{self.config.output_routing_key_period_2}"
        return None

    def _handle_data_message(self, _, client_id, gateway_id, payload):
        """
        Handle incoming data messages by classifying transactions and sending them to the output exchange with the appropriate routing keys.
        """
        transactions_by_routing_key = {
            routing_key: []
            for routing_key in [
                f"{self.config.output_routing_key_usd}.{self.config.output_routing_key_period_1}",
                f"{self.config.output_routing_key_usd}.{self.config.output_routing_key_period_2}",
                f"{self.config.output_routing_key_no_usd}.{self.config.output_routing_key_period_1}",
            ]
        }
        for transaction in payload:
            routing_key = self._classify_transaction(transaction)
            if routing_key is not None:
                transactions_by_routing_key[routing_key].append(transaction)
        for routing_key, transactions in transactions_by_routing_key.items():
            self._output_exchange.send(
                internal.serialize_msg(
                    internal.MsgType.TRANSACTION_BATCH,
                    client_id,
                    gateway_id,
                    transactions,
                ),
                routing_key=routing_key,
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
