import logging

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.models.count import Count
from common.models.eof import EOF
from common.protocol import internal
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from config import Config


class LowAmountReducer(StatefulCoordinatedWorker):
    def __init__(self, config: Config):
        """
        Initialize the LowAmountReducer with the given configuration, setting up the counter dictionary.
        """
        super().__init__()
        self.config = config
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
        """
        Return the input queue to consume messages from the previous stage.
        """
        return self._input_queue

    @property
    def _output_middleware(self):
        """
        Return the output exchange to forward messages to the next stage.
        """
        return self._output_exchange

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

    def _ring_routing_key(self, node_id):
        return f"{self.config.node_prefix}{node_id}"

    def _flush_data(self, client_id, gateway_id):
        """
        Flush any buffered data by sending the count of low amount transactions for the given client_id and gateway_id to the output exchange.
        """
        count = self._counts.pop((client_id, gateway_id), 0)
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.COUNT, client_id, gateway_id, Count(count=count)
            ),
            routing_key=gateway_id,
        )

    def _send_final_eof(self, client_id, gateway_id, eof):
        """
        Send the final EOF message to the next stage's output exchange with the appropriate routing key.
        """
        self._output_exchange.send(
            internal.serialize_msg(internal.MsgType.EOF, client_id, gateway_id, eof),
            routing_key=gateway_id,
        )

    def _handle_data_message(self, msg_type, client_id, gateway_id, payload):
        """
        Handle incoming data messages by accumulating the count of low amount transactions.
        """
        if msg_type != internal.MsgType.COUNT:
            return
        key = (client_id, gateway_id)
        self._counts[key] = self._counts.get(key, 0) + payload.count
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
