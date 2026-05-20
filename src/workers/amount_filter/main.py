import logging
import threading

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareExchangeTopicRabbitMQ,
)
from common.models.query_results import Q1Result
from common.protocol import internal
from common.worker.stateless_coordinated_worker import StatelessCoordinatedWorker
from config import Config

QUERY_ID = 1


class AmountFilter(StatelessCoordinatedWorker):
    def __init__(self, config):
        super().__init__()
        self.config = config

        self._input_exchange = MessageMiddlewareExchangeTopicRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            binding_patterns=[
                config.input_routing_key,
                config.input_eof_routing_key,
            ],
            queue_name=config.input_queue_name,
        )
        self._output_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
            routing_keys=[],
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

    def _handle_data_message(self, _, client_id, gateway_id, payload):
        super()._handle_data_message(_, client_id, gateway_id, payload)

        filtered = [
            Q1Result(
                from_bank=tx.from_bank,
                from_account=tx.from_account,
                to_bank=tx.to_bank,
                to_account=tx.to_account,
                amount_paid=tx.amount,
            )
            for tx in payload
            if tx.amount < self.config.amount_threshold
        ]
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.Q1_RESULT_BATCH, client_id, gateway_id, filtered
            ),
            routing_key=gateway_id,
        )

    def _send_final_eof(self, client_id, gateway_id, eof):
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.QUERY_END,
                client_id,
                gateway_id,
                QUERY_ID,
                eof.message_count,
            ),
            routing_key=gateway_id,
        )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [AmountFilter] %(levelname)s %(message)s",
    )
    config = Config.from_env()
    worker = AmountFilter(config)
    try:
        worker.start()
    except Exception as e:
        logging.error("Error during AmountFilter execution: %s", e)
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
