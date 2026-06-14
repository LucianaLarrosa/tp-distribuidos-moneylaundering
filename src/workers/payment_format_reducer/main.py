import logging

from common.ids import flush_id, eof_id
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareExchangeFanoutRabbitMQ,
)
from common.models.payment_format_average import PaymentFormatAverage
from common.protocol.internal import internal
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from config import Config


class PaymentFormatReducer(StatefulCoordinatedWorker):
    def __init__(self, config):
        self.config = config
        super().__init__()
        self._totals = {}

        self._input_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            routing_keys=[config.shard_id],
            queue_name=f"{config.input_exchange}_{config.shard_id}",
        )
        self._output_exchange = MessageMiddlewareExchangeFanoutRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
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

    def _flow_key(self, client_id, gateway_id):
        return (client_id, gateway_id)

    def _handle_data_message(self, _, client_id, gateway_id, partial_batch):
        super()._handle_data_message(_, client_id, gateway_id, partial_batch)
        delta = {}
        for partial in partial_batch:
            total_amount, count = delta.get(partial.payment_format, (0.0, 0))
            delta[partial.payment_format] = [
                total_amount + partial.total_amount,
                count + partial.count,
            ]
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
        batch = []
        for payment_format, totals in flow_totals.items():
            total_amount, count = totals
            if count == 0:
                continue
            batch.append(
                PaymentFormatAverage(
                    payment_format=payment_format,
                    average_amount=total_amount / count,
                )
            )
        self._send_average_batch(client_id, gateway_id, batch)

    def _send_average_batch(self, client_id, gateway_id, batch):
        self._send(
            self._output_exchange,
            internal.MsgType.PAYMENT_FORMAT_AVERAGE_BATCH,
            client_id,
            gateway_id,
            batch,
            message_id=flush_id(self.config.node_id, client_id, gateway_id, 0),
        )
        self._increment_sent_count(client_id, gateway_id)

    def _send_final_eof(self, client_id, gateway_id, eof):
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.EOF,
                client_id,
                gateway_id,
                eof,
                message_id=eof_id(client_id, gateway_id),
            )
        )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [PaymentFormatReducer] %(levelname)s %(message)s",
    )
    config = Config()
    reducer = PaymentFormatReducer(config)
    try:
        reducer.start()
    except Exception as e:
        logging.error(f"Error during PaymentFormatReducer execution: {e}")
    finally:
        reducer.shutdown()


if __name__ == "__main__":
    main()
