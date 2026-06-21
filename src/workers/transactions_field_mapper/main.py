import logging
from datetime import datetime

from common.ids import eof_id, final_eof_id
from common.models.transaction import Transaction
from common.protocol.internal import internal
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareExchangeTopicRabbitMQ,
)
from common.sharding import shard_of
from common.worker.stateless_worker import StatelessWorker
from config import Config

BANK_MAX_EOF_SHARD = "0"


class TransactionsFieldMapper(StatelessWorker):
    def __init__(self, config):
        super().__init__(config)

        self._input_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.raw_data_exchange,
            routing_keys=[config.input_routing_key],
            queue_name=config.input_queue_name,
        )
        self._output_exchange = MessageMiddlewareExchangeTopicRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
            binding_patterns=[],
        )
        self._bank_max_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.bank_max_exchange,
            routing_keys=[],
        )

    @property
    def _input_middleware(self):
        return self._input_exchange

    @property
    def _output_middleware(self):
        return self._output_exchange

    def _send_final_eof(self, client_id, gateway_id, eof):
        msg = internal.serialize_msg(
            internal.MsgType.EOF,
            client_id,
            gateway_id,
            eof,
            message_id=final_eof_id(client_id, gateway_id, eof),
        )
        self._output_exchange.send(msg, routing_key=self.config.output_routing_key_eof)
        self._bank_max_exchange.send(msg, routing_key=BANK_MAX_EOF_SHARD)

    def shutdown(self):
        super().shutdown()
        self._bank_max_exchange.close()

    def _handle_data_message(self, _, client_id, gateway_id, payload):
        """
        Parse the raw batch into Transactions and publish it to the USD-only
        route and the all-transactions route.
        """
        transactions = [self._parse(raw_tx.raw) for raw_tx in payload]

        usd_transactions = []
        for tx in transactions:
            if tx.currency.lower() == self.config.usd_currency.lower():
                usd_transactions.append(tx)

        self._send(
            self._output_exchange,
            internal.MsgType.TRANSACTION_BATCH,
            client_id,
            gateway_id,
            usd_transactions,
            routing_key=self.config.output_routing_key_usd,
        )
        self._send(
            self._output_exchange,
            internal.MsgType.TRANSACTION_BATCH,
            client_id,
            gateway_id,
            transactions,
            routing_key=self.config.output_routing_key_all,
        )
        self._send(
            self._bank_max_exchange,
            internal.MsgType.TRANSACTION_BATCH,
            client_id,
            gateway_id,
            usd_transactions,
            routing_key=str(
                shard_of(self._current_message_id, self.config.bank_max_node_count)
            ),
        )

    def _parse(self, raw):
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
    config = Config()
    worker = TransactionsFieldMapper(config)
    try:
        worker.start()
    except Exception as e:
        logging.error(f"Error during TransactionsFieldMapper execution: {e}")
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
