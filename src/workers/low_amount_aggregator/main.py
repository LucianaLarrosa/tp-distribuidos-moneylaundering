import logging

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.models.count import Count
from common.protocol import internal
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from config import Config


class LowAmountAggregator(StatefulCoordinatedWorker):
    def __init__(self, config: Config):
        """
        Initializes the LowAmountAggregator worker with the given configuration, setting up the counter dictionary.
        """
        super().__init__()
        self.config = config
        self._counts = {}  # (client_id, gateway_id) -> count

        self._input_queue = MessageMiddlewareQueueRabbitMQ(
            host=config.rabbitmq_host,
            queue_name=config.input_queue,
        )
        self._output_queue = MessageMiddlewareQueueRabbitMQ(
            host=config.rabbitmq_host,
            queue_name=config.output_queue,
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
    def _input_control_middleware(self):
        """
        Return the input control exchange to consume control messages.
        """
        return self._input_control_exchange

    @property
    def _output_control_middleware(self):
        """
        Return the output control exchange to send control messages from the main thread.
        """
        return self._output_control_exchange

    @property
    def _control_output_control_middleware(self):
        """
        Return the output control exchange to send control messages from the control thread.
        """
        return self._control_output_control_exchange

    @property
    def _input_middleware(self):
        """
        Return the input queue to consume messages from the previous stage.
        """
        return self._input_queue

    @property
    def _output_middleware(self):
        """
        Return the output queue to forward messages to the next stage.
        """
        return self._output_queue

    def _ring_routing_key(self, node_id):
        return f"{self.config.node_prefix}{node_id}"

    def _flush_data(self, client_id, gateway_id):
        """
        Flush any buffered data by sending the count of low amount transactions for the given client_id and gateway_id to the output queue.
        """
        count = self._counts.pop((client_id, gateway_id), 0)
        self._output_queue.send(
            internal.serialize_msg(
                internal.MsgType.COUNT, client_id, gateway_id, Count(count=count)
            )
        )

    def _handle_data_message(self, _, client_id, gateway_id, payload):
        """
        Handle incoming data messages by counting the number of transactions with amounts below the configured maximum amount.
        """
        client_gateway_key = (client_id, gateway_id)
        transaction_count = sum(
            1 for transaction in payload if transaction.amount < self.config.max_amount
        )
        self._counts[client_gateway_key] = (
            self._counts.get(client_gateway_key, 0) + transaction_count
        )
        super()._handle_data_message(_, client_id, gateway_id, payload)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [LowAmountAggregator] %(levelname)s %(message)s",
    )
    config = Config()
    aggregator = LowAmountAggregator(config)
    try:
        aggregator.start()
    except Exception as e:
        logging.error(f"Error during LowAmountAggregator execution: {e}")
    finally:
        aggregator.shutdown()


if __name__ == "__main__":
    main()
