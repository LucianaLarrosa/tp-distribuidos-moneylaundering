import logging
from datetime import datetime

from common.models.transaction import Transaction
from common.protocol import internal
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareExchangeTopicRabbitMQ,
)
from common.worker.stateless_worker import StatelessWorker
from config import Config


class TransactionsFieldMapper(StatelessWorker):
    def __init__(self, config):
        """
        Initialize the TransactionsFieldMapper worker with the given configuration.
        """
        super().__init__()
        self.config = config

        self._input_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.raw_data_exchange,
            routing_keys=[config.input_routing_key],
        )
        self._output_exchange = MessageMiddlewareExchangeTopicRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
            binding_patterns=[],
        )

    @property
    def _input_middleware(self):
        """
        Return the input exchange to consume raw transactions from the gateway.
        """
        return self._input_exchange

    @property
    def _output_middleware(self):
        """
        Return the output topic exchange to forward filtered transactions downstream.
        """
        return self._output_exchange

    def _send_final_eof(self, client_id, gateway_id, eof):
        """
        Send the EOF to both output routing keys so all downstream consumers see it.
        """
        msg = internal.serialize_msg(
            internal.MsgType.EOF, client_id, gateway_id, eof
        )
        self._output_exchange.send(msg, routing_key=self.config.output_routing_key_usd)
        self._output_exchange.send(msg, routing_key=self.config.output_routing_key_all)

    def _handle_data_message(self, _, client_id, gateway_id, payload):
        """
        Parse the raw batch into Transactions and publish to both routing keys.
        """
        transactions = [self._parse(raw_tx.raw) for raw_tx in payload]
        usd_transactions = [
            tx for tx in transactions if tx.currency.lower() == self.config.usd_currency
        ]
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.TRANSACTION_BATCH,
                client_id,
                gateway_id,
                usd_transactions,
            ),
            routing_key=self.config.output_routing_key_usd,
        )
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.TRANSACTION_BATCH,
                client_id,
                gateway_id,
                transactions,
            ),
            routing_key=self.config.output_routing_key_all,
        )

    def _parse(self, raw):
        """
        Parse a CSV line into a Transaction dataclass.
        """
        fields = raw.split(",")
        return Transaction(
            timestamp=datetime.strptime(fields[0], "%Y/%m/%d %H:%M"),
            from_bank=fields[1],
            from_account=fields[2],
            to_bank=fields[3],
            to_account=fields[4],
            amount=float(fields[7]),
            currency=fields[8],
            payment_format=fields[9],
        )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [TransactionsFieldMapper] %(levelname)s %(message)s",
    )
    config = Config.from_env()
    worker = TransactionsFieldMapper(config)
    try:
        worker.start()
    except Exception as e:
        logging.error(f"Error during TransactionsFieldMapper execution: {e}")
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
