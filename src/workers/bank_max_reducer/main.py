import logging

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)

from common.protocol.internal import internal
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from config import Config


class BankMaxReducer(StatefulCoordinatedWorker):
    def __init__(self, config):
        self.config = config
        super().__init__()
        self._global_max = {}  # (client_id, gateway_id) -> {from_bank: BankMaxPartial}

        self._input_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            routing_keys=[config.shard_id],
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

    def _handle_data_message(self, _, client_id, gateway_id, bank_max_batch):
        super()._handle_data_message(_, client_id, gateway_id, bank_max_batch)
        flow_max = self._global_max.setdefault((client_id, gateway_id), {})
        for bank_max in bank_max_batch:
            current = flow_max.get(bank_max.from_bank)
            if (
                current is None
                or bank_max.amount > current.amount
                or (
                    bank_max.amount == current.amount
                    and bank_max.from_account < current.from_account
                )
            ):
                flow_max[bank_max.from_bank] = bank_max

    def _flush_data(self, client_id, gateway_id):
        flow_max = self._global_max.pop((client_id, gateway_id), {})
        self._flush_sharded(
            self._output_exchange,
            internal.MsgType.BANK_MAX_PARTIAL_BATCH,
            client_id,
            gateway_id,
            list(flow_max.values()),
            key_of=lambda bank_max: bank_max.from_bank,
            num_shards=self.config.output_node_count,
            batch_size=self.config.batch_size,
        )

    def _send_final_eof(self, client_id, gateway_id, eof):
        self._output_exchange.send(
            internal.serialize_msg(internal.MsgType.EOF, client_id, gateway_id, eof),
            routing_key="1",
        )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [BankMaxReducer] %(levelname)s %(message)s",
    )
    config = Config()
    reducer = BankMaxReducer(config)
    try:
        reducer.start()
    except Exception as e:
        logging.error(f"Error during BankMaxReducer execution: {e}")
    finally:
        reducer.shutdown()


if __name__ == "__main__":
    main()
