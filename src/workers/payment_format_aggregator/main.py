import logging

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.ids import eof_id
from common.models.payment_format_partial import PaymentFormatPartial
from common.protocol.internal import internal
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from config import Config


class PaymentFormatAggregator(StatefulCoordinatedWorker):
    def __init__(self, config):
        super().__init__(config)
        self._totals = {}

        self._input_queue = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            routing_keys=[str(config.node_id)],
            queue_name=config.input_queue,
        )
        self._output_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
            routing_keys=[],
        )

    @property
    def _input_middleware(self):
        return self._input_queue

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

    def _shard_routing_key(self, shard_id):
        return str(shard_id)

    def _flow_key(self, client_id, gateway_id):
        return (client_id, gateway_id)

    def _payment_format_key(self, payment_format):
        return payment_format.strip().lower()

    def _has_required_fields(self, tx):
        return tx.payment_format is not None and tx.amount is not None

    def _handle_data_message(self, _, client_id, gateway_id, transaction_batch):
        super()._handle_data_message(_, client_id, gateway_id, transaction_batch)
        delta = {}
        for transaction in transaction_batch:
            if not self._has_required_fields(transaction):
                continue
            payment_format = self._payment_format_key(transaction.payment_format)
            total_amount, count = delta.get(payment_format, (0.0, 0))
            delta[payment_format] = [total_amount + transaction.amount, count + 1]
        self._apply_delta(client_id, gateway_id, delta)
        return delta

    def _apply_delta(self, client_id, gateway_id, delta):
        flow_totals = self._totals.setdefault(self._flow_key(client_id, gateway_id), {})
        for payment_format, (total_amount, count) in delta.items():
            cur_total, cur_count = flow_totals.get(payment_format, (0.0, 0))
            flow_totals[payment_format] = (cur_total + total_amount, cur_count + count)

    def _state_as_delta(self, client_id, gateway_id):
        return {
            payment_format: [total_amount, count]
            for payment_format, (total_amount, count) in self._totals.get(
                self._flow_key(client_id, gateway_id), {}
            ).items()
        }

    def _flush_data(self, client_id, gateway_id):
        flow_totals = self._totals.pop(self._flow_key(client_id, gateway_id), {})
        partials = [
            PaymentFormatPartial(
                payment_format=payment_format,
                total_amount=total_amount,
                count=count,
            )
            for payment_format, (total_amount, count) in flow_totals.items()
        ]
        self._flush_sharded(
            self._output_exchange,
            internal.MsgType.PAYMENT_FORMAT_PARTIAL_BATCH,
            client_id,
            gateway_id,
            partials,
            key_of=lambda partial: partial.payment_format,
            num_shards=self.config.num_shards,
            batch_size=self.config.batch_size,
        )

    def _send_final_eof(self, client_id, gateway_id, eof):
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.EOF,
                client_id,
                gateway_id,
                eof,
                message_id=eof_id(client_id, gateway_id),
            ),
            routing_key=self._shard_routing_key(0),
        )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [PaymentFormatAggregator] %(levelname)s %(message)s",
    )
    config = Config()
    aggregator = PaymentFormatAggregator(config)
    try:
        aggregator.start()
    except Exception as e:
        logging.error(f"Error during PaymentFormatAggregator execution: {e}")
    finally:
        aggregator.shutdown()


if __name__ == "__main__":
    main()
