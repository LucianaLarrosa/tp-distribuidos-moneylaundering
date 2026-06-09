import logging

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareQueueRabbitMQ,
)
from common.models.count import Count
from common.protocol.internal import internal
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from config import Config


class LowAmountAggregator(StatefulCoordinatedWorker):
    def __init__(self, config: Config):
        super().__init__(config)
        self._counts = {}  # (client_id, gateway_id) -> count

        self._input_queue = MessageMiddlewareQueueRabbitMQ(
            host=config.rabbitmq_host,
            queue_name=config.input_queue,
        )
        self._output_queue = MessageMiddlewareQueueRabbitMQ(
            host=config.rabbitmq_host,
            queue_name=config.output_queue,
        )

    @property
    def _input_middleware(self):
        return self._input_queue

    @property
    def _output_middleware(self):
        return self._output_queue

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
        self._increment_sent_count(client_id, gateway_id)

    def _send_final_eof(self, client_id, gateway_id, eof):
        self._output_queue.send(
            internal.serialize_msg(internal.MsgType.EOF, client_id, gateway_id, eof)
        )

    def _handle_data_message(self, _, client_id, gateway_id, payload):
        """
        Handle incoming data messages by counting the number of transactions with amounts below the configured maximum amount.
        """
        client_gateway_key = (client_id, gateway_id)
        transaction_count = sum(
            1
            for transaction in payload
            if transaction.amount < self.config.amount_threshold
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
