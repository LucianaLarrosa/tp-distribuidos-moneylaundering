import threading
from abc import abstractmethod

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.worker.worker import Worker
from common.protocol.internal import internal
from common.models.eof import EOF, RingEOF


class RingCoordinatedWorker(Worker):
    def __init__(self, config):
        super().__init__(config)
        self._processed_counts = (
            {}
        )  # (client_id, gateway_id) -> processed_count | actual total processed count
        self._partial_processed_count = (
            {}
        )  # (client_id, gateway_id) -> partial_processed_count | previous process count sent to ring
        self._processed_counts_lock = threading.Lock()
        self._control_thread = None

        self._input_control_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=self._rabbitmq_host,
            exchange_name=self._control_exchange_name,
            routing_keys=[self._get_ring_routing_key(self._node_id)],
        )
        self._output_control_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=self._rabbitmq_host,
            exchange_name=self._control_exchange_name,
            routing_keys=[],
        )
        self._control_output_control_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=self._rabbitmq_host,
            exchange_name=self._control_exchange_name,
            routing_keys=[],
        )

    @property
    @abstractmethod
    def _rabbitmq_host(self):
        pass

    @property
    @abstractmethod
    def _control_exchange_name(self):
        pass

    @property
    @abstractmethod
    def _node_prefix(self):
        pass

    @property
    @abstractmethod
    def _node_id(self):
        pass

    @property
    @abstractmethod
    def _ring_size(self):
        pass

    @abstractmethod
    def _get_total_sent_count(self, _client_id, _gateway_id, current_total):
        pass

    @abstractmethod
    def _flush_data(self, client_id, gateway_id):
        pass

    @abstractmethod
    def _get_final_eof_count(self, ring_eof):
        pass

    @property
    def _input_control_middleware(self):
        return self._input_control_exchange

    @property
    def _output_control_middleware(self):
        return self._output_control_exchange

    @property
    def _control_output_control_middleware(self):
        return self._control_output_control_exchange

    def _get_ring_routing_key(self, node_id):
        return f"{self._node_prefix}{node_id}"

    def _get_total_count(self, key, count_dict, partial_dict, lock, current_total):
        with lock:
            count = count_dict.get(key, 0)
            partial = partial_dict.get(key, 0)
            partial_dict[key] = count
        return current_total + count - partial

    def _increment_processed_count(self, client_id, gateway_id):
        with self._processed_counts_lock:
            self._processed_counts[(client_id, gateway_id)] = (
                self._processed_counts.get((client_id, gateway_id), 0) + 1
            )

    def _update_ring_eof(self, client_id, gateway_id, ring_eof):
        """
        Update the RING_EOF message with the processed and sent counts and determine if this node should be the coordinator.
        """
        total_processed_count = self._get_total_count(
            (client_id, gateway_id),
            self._processed_counts,
            self._partial_processed_count,
            self._processed_counts_lock,
            ring_eof.total_processed_count,
        )
        coordinator_id = (
            self._node_id if total_processed_count >= ring_eof.expected_count else None
        )
        return RingEOF(
            expected_count=ring_eof.expected_count,
            total_processed_count=total_processed_count,
            coordinator_id=coordinator_id,
            total_sent_count=self._get_total_sent_count(
                client_id, gateway_id, ring_eof.total_sent_count or 0
            ),
        )

    def _handle_control_eof_message(
        self, client_id, gateway_id, ring_eof, output_exchange=None
    ):
        """
        Handle a RING_EOF message by either forwarding it to the next node in the ring, flushing data and sending an EOF to the output middleware if this node is the coordinator.
        """
        if output_exchange is None:
            output_exchange = self._control_output_control_middleware

        if ring_eof.coordinator_id is None:
            ring_eof = self._update_ring_eof(client_id, gateway_id, ring_eof)
        else:
            self._flush_data(client_id, gateway_id)
            ring_eof.total_sent_count = self._get_total_sent_count(
                client_id, gateway_id, ring_eof.total_sent_count or 0
            )
            if ring_eof.coordinator_id == self._node_id:
                self._send_final_eof(
                    client_id, gateway_id, EOF(self._get_final_eof_count(ring_eof))
                )
                return
        output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.RING_EOF, client_id, gateway_id, ring_eof
            ),
            routing_key=self._get_ring_routing_key(self._get_next_node_id()),
        )

    def _handle_control_message(self, message, ack, nack):
        try:
            _, client_id, gateway_id, ring_eof = internal.deserialize_msg(message)
            self._handle_control_eof_message(client_id, gateway_id, ring_eof)
            ack()
        except Exception:
            nack()
            raise

    def _get_next_node_id(self):
        return (self._node_id + 1) % self._ring_size

    def _handle_eof_message(self, client_id, gateway_id, eof, output_exchange=None):
        """
        Handle an EOF message by sending a RING_EOF to the next node in the ring.
        """
        if output_exchange is None:
            output_exchange = self._output_control_middleware

        total_processed_count = self._get_total_count(
            (client_id, gateway_id),
            self._processed_counts,
            self._partial_processed_count,
            self._processed_counts_lock,
            0,
        )
        output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.RING_EOF,
                client_id,
                gateway_id,
                RingEOF(
                    expected_count=eof.message_count,
                    total_processed_count=total_processed_count,
                    total_sent_count=self._get_total_sent_count(
                        client_id, gateway_id, 0
                    ),
                ),
            ),
            routing_key=self._get_ring_routing_key(self._get_next_node_id()),
        )

    def _handle_data_message(self, msg_type, client_id, gateway_id, payload):
        self._increment_processed_count(client_id, gateway_id)

    def start(self):
        self._control_thread = threading.Thread(
            target=self._input_control_middleware.start_consuming,
            args=(self._handle_control_message,),
            daemon=True,
        )
        self._control_thread.start()
        super().start()

    def shutdown(self):
        super().shutdown()
        if self._control_thread and self._control_thread.is_alive():
            self._input_control_middleware.stop_consuming_threadsafe()
            self._control_thread.join()
        self._input_control_exchange.close()
        self._output_control_exchange.close()
        self._control_output_control_exchange.close()
