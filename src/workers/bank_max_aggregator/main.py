import hashlib
import logging

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareExchangeTopicRabbitMQ,
)
from common.models.bank_max_partial import BankMaxPartial
from common.protocol import internal
from common.worker.sent_coordinated_worker import SentCoordinatedWorker
from config import Config


class BankMaxAggregator(SentCoordinatedWorker):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self._local_max = (
            {}
        )  # (client_id, gateway_id) -> {from_bank: tx_with_max_amount}

        self._input_queue = MessageMiddlewareExchangeTopicRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            binding_patterns=config.input_binding_patterns,
            queue_name=config.input_queue,
        )
        self._output_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
            routing_keys=[self._shard_routing_key(s) for s in range(config.num_shards)],
        )
        self._input_control_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.control_exchange,
            routing_keys=[self._ring_routing_key(config.node_id)],
        )
        self._output_control_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.control_exchange,
            routing_keys=[],
        )
        self._control_output_control_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.control_exchange,
            routing_keys=[],
        )

    @property
    def _node_id(self):
        return self.config.node_id

    @property
    def _ring_size(self):
        return self.config.ring_size

    @property
    def _input_middleware(self):
        return self._input_queue

    @property
    def _output_middleware(self):
        return self._output_exchange

    @property
    def _input_control_middleware(self):
        return self._input_control_exchange

    @property
    def _output_control_middleware(self):
        return self._output_control_exchange

    @property
    def _control_output_control_middleware(self):
        return self._control_output_control_exchange

    def _ring_routing_key(self, node_id):
        return f"{self.config.node_prefix}{node_id}"

    def _shard_routing_key(self, shard_id):
        return str(shard_id)

    def _handle_data_message(self, _, client_id, gateway_id, transaction_batch):
        super()._handle_data_message(_, client_id, gateway_id, transaction_batch)
        flow_max = self._local_max.setdefault((client_id, gateway_id), {})
        for transaction in transaction_batch:
            current = flow_max.get(transaction.from_bank)
            if current is None or transaction.amount > current.amount:
                flow_max[transaction.from_bank] = BankMaxPartial(
                    from_bank=transaction.from_bank,
                    from_account=transaction.from_account,
                    amount=transaction.amount,
                )

    def _flush_data(self, client_id, gateway_id):
        flow_max = self._local_max.pop((client_id, gateway_id), {})
        pending = {}
        for from_bank, transaction in flow_max.items():
            shard_id = (
                int(hashlib.md5(str(from_bank).encode()).hexdigest(), 16)
                % self.config.num_shards
            )
            bucket = pending.setdefault(shard_id, [])
            bucket.append(transaction)
            if len(bucket) >= self.config.batch_size:
                self._send_shard_batch(client_id, gateway_id, shard_id, bucket)
                pending[shard_id] = []
        for shard_id, bucket in pending.items():
            if bucket:
                self._send_shard_batch(client_id, gateway_id, shard_id, bucket)

    def _send_shard_batch(self, client_id, gateway_id, shard_id, batch):
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.BANK_MAX_PARTIAL_BATCH, client_id, gateway_id, batch
            ),
            routing_key=self._shard_routing_key(shard_id),
        )
        self._increment_sent_count(client_id, gateway_id)

    def _send_final_eof(self, client_id, gateway_id, eof):
        self._output_exchange.send(
            internal.serialize_msg(internal.MsgType.EOF, client_id, gateway_id, eof),
            routing_key=self._shard_routing_key(0),
        )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [BankMaxAggregator] %(levelname)s %(message)s",
    )
    config = Config()
    aggregator = BankMaxAggregator(config)
    try:
        aggregator.start()
    except Exception as e:
        logging.error(f"Error during BankMaxAggregator execution: {e}")
    finally:
        aggregator.shutdown()


if __name__ == "__main__":
    main()
