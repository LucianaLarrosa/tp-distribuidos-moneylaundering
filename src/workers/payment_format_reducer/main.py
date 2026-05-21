import logging

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareExchangeFanoutRabbitMQ,
)
from common.models.payment_format_average import PaymentFormatAverage
from common.protocol import internal
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from config import Config


class PaymentFormatReducer(StatefulCoordinatedWorker):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self._totals = {}

        self._input_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            routing_keys=[config.shard_id],
        )
        self._output_exchange = MessageMiddlewareExchangeFanoutRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
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
        return self._input_exchange

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

    def _flow_key(self, client_id, gateway_id):
        return (client_id, gateway_id)

    def _handle_data_message(self, _, client_id, gateway_id, partial_batch):
        super()._handle_data_message(_, client_id, gateway_id, partial_batch)
        flow_totals = self._totals.setdefault(self._flow_key(client_id, gateway_id), {})
        for partial in partial_batch:
            total_amount, count = flow_totals.get(partial.payment_format, (0.0, 0))
            flow_totals[partial.payment_format] = (
                total_amount + partial.total_amount,
                count + partial.count,
            )

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
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.PAYMENT_FORMAT_AVERAGE_BATCH,
                client_id,
                gateway_id,
                batch,
            )
        )

    def _send_final_eof(self, client_id, gateway_id, eof):
        self._output_exchange.send(
            internal.serialize_msg(internal.MsgType.EOF, client_id, gateway_id, eof)
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
