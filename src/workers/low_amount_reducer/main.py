import logging

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.models.query_results import Q5Result
from common.protocol.internal import internal
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from config import Config


class LowAmountReducer(StatefulCoordinatedWorker):
    def __init__(self, config: Config):
        super().__init__(config)
        self._counts = {}  # (client_id, gateway_id) -> accumulated count

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
        Flush any buffered data by sending a Q5 result batch with the accumulated count.
        """
        count = self._counts.pop((client_id, gateway_id), 0)
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.Q5_RESULT_BATCH,
                client_id,
                gateway_id,
                [Q5Result(count=count)],
            ),
            routing_key=gateway_id,
        )
        self._increment_sent_count(client_id, gateway_id)

    def _send_final_eof(self, client_id, gateway_id, eof):
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.QUERY_END,
                client_id,
                gateway_id,
                self.config.query_id,
                eof.message_count,
            ),
            routing_key=gateway_id,
        )

    def _handle_data_message(self, msg_type, client_id, gateway_id, payload):
        """
        Handle incoming data messages by accumulating the count of low amount transactions.
        """
        client_gateway_key = (client_id, gateway_id)
        self._counts[client_gateway_key] = (
            self._counts.get(client_gateway_key, 0) + payload.count
        )
        super()._handle_data_message(msg_type, client_id, gateway_id, payload)


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
