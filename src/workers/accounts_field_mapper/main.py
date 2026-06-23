import logging

from common.ids import eof_id, final_eof_id
from common.models.bank import Bank
from common.protocol.internal import internal
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.sharding import shard_of
from common.worker.stateless_worker import StatelessWorker
from config import Config


class AccountsFieldMapper(StatelessWorker):
    def __init__(self, config):
        super().__init__(config)

        self._input_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.raw_data_exchange,
            routing_keys=[config.input_routing_key],
            queue_name=config.input_queue_name,
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

    def _send_final_eof(self, client_id, eof):
        for rk in self.config.output_routing_keys:
            self._output_exchange.send(
                internal.serialize_msg(
                    internal.MsgType.EOF,
                    client_id,
                    eof,
                    message_id=final_eof_id(client_id, eof),
                ),
                routing_key=self._output_prefix_routing_key(rk),
            )

    def _handle_data_message(self, _, client_id, payload):
        """
        Parse each raw CSV line into a Bank and send one batch to every shard.
        The input id is forwarded unchanged (pass-through): each shard goes to a
        distinct downstream node, so the input id is already unique per consumer.
        """
        banks = [self._parse(raw_acc.raw) for raw_acc in payload]

        sharded_banks = {
            node_id: [] for node_id in range(self.config.output_node_count)
        }
        for bank in banks:
            sharded_banks[shard_of(bank.bank_id, self.config.output_node_count)].append(
                bank
            )

        for node_id, bank_list in sharded_banks.items():
            self._send(
                self._output_exchange,
                internal.MsgType.BANK_BATCH,
                client_id,
                bank_list,
                routing_key=self._output_prefix_routing_key(node_id),
            )

    def _output_prefix_routing_key(self, routing_key):
        return f"{self.config.output_node_prefix}{routing_key}"

    def _parse(self, raw):
        fields = raw.split(",")
        return Bank(bank_id=fields[1], name=fields[0])


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [AccountsFieldMapper] %(levelname)s %(message)s",
    )
    config = Config()
    worker = AccountsFieldMapper(config)
    try:
        worker.start()
    except Exception as e:
        logging.error(f"Error during AccountsFieldMapper execution: {e}")
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
