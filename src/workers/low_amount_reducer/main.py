import logging

from common.idempotency.ids import flush_id, eof_id, final_eof_id
from common.communication.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.models.query_results import Q5Result
from common.communication.protocol import internal
from common.worker.stateful_worker import StatefulWorker
from config import Config


class LowAmountReducer(StatefulWorker):
    def __init__(self, config: Config):
        super().__init__(config)
        self._counts = {}  # (client_id) -> accumulated count

        self._input_queue = MessageMiddlewareQueueRabbitMQ(
            host=config.rabbitmq_host,
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






    def _flush_data(self, client_id):
        """
        Flush any buffered data by sending a Q5 result batch with the accumulated count.
        """
        count = self._counts.pop(client_id, 0)
        self._send(
            self._output_exchange,
            internal.MsgType.Q5_RESULT_BATCH,
            client_id,
            [Q5Result(count=count)],
            routing_key=client_id,
            message_id=flush_id(self.config.node_id, client_id, 0),
        )
        self._increment_sent_count(client_id)

    def _send_final_eof(self, client_id, eof):
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.QUERY_END,
                client_id,
                self.config.query_id,
                eof.message_count,
                message_id=final_eof_id(client_id, eof, self.config.query_id),
            ),
            routing_key=client_id,
        )

    def _handle_data_message(self, msg_type, client_id, payload):
        """
        Handle incoming data messages by accumulating the count of low amount transactions.
        """
        self._counts[client_id] = (
            self._counts.get(client_id, 0) + payload.count
        )
        super()._handle_data_message(msg_type, client_id, payload)
        return {"count": payload.count}

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
        format="%(asctime)s [LowAmountReducer] %(levelname)s %(message)s",
    )
    config = Config()
    reducer = LowAmountReducer(config)
    try:
        reducer.start()
    except Exception as e:
        logging.error(f"Error during LowAmountReducer execution: {e}")
    finally:
        reducer.shutdown()


if __name__ == "__main__":
    main()
