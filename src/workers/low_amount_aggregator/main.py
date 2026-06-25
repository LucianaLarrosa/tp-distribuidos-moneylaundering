import logging

from common.idempotency.ids import flush_id, eof_id, final_eof_id
from common.communication.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.models.count import Count
from common.communication.protocol import internal
from common.worker.stateful_worker import StatefulWorker
from config import Config


class LowAmountAggregator(StatefulWorker):
    def __init__(self, config: Config):
        super().__init__(config)
        self._counts = {}  # (client_id) -> count

        self._input_queue = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            routing_keys=[str(config.node_id)],
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






    def _flush_data(self, client_id):
        """
        Flush any buffered data by sending the count of low amount transactions for the given client_id to the output queue.
        """
        count = self._counts.pop(client_id, 0)
        self._send(
            self._output_queue,
            internal.MsgType.COUNT,
            client_id,
            Count(count=count),
            message_id=flush_id(self.config.node_id, client_id, 0),
        )
        self._increment_sent_count(client_id)

    def _send_final_eof(self, client_id, eof):
        self._output_queue.send(
            internal.serialize_msg(
                internal.MsgType.EOF,
                client_id,
                eof,
                message_id=final_eof_id(client_id, eof),
            )
        )

    def _handle_data_message(self, _, client_id, payload):
        """
        Handle incoming data messages by counting the number of transactions with amounts below the configured maximum amount.
        """
        transaction_count = sum(
            1
            for transaction in payload
            if transaction.amount < self.config.amount_threshold
        )
        self._counts[client_id] = (
            self._counts.get(client_id, 0) + transaction_count
        )
        super()._handle_data_message(_, client_id, payload)
        return {"count": transaction_count}

    def _cleanup_state(self, client_id):
        super()._cleanup_state(client_id)
        self._counts.pop(client_id, None)

    def _apply_delta(self, client_id, delta):
        self._counts[client_id] = (
            self._counts.get(client_id, 0) + delta["count"]
        )

    def _state_as_delta(self, client_id):
        return {"count": self._counts.get(client_id, 0)}


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
