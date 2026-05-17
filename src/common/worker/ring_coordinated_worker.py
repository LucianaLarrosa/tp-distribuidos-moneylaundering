import threading
from abc import abstractmethod

from common.worker.worker import Worker
from common.protocol import internal
from common.models.eof import EOF, RingEOF


class RingCoordinatedWorker(Worker):
    def __init__(self) -> None:
        """
        Initialize the ring-coordinated worker with a dictionary to track processed counts and a lock to synchronize access to this dictionary. Also initialize a control thread to listen for RING_EOF messages.
        """
        super().__init__()
        self._processed_counts = (
            {}
        )  # (client_id, gateway_id) -> processed_count | actual total processed count
        self._partial_processed_count = (
            {}
        )  # (client_id, gateway_id) -> partial_processed_count | previous process count sent to ring
        self._processed_counts_lock = threading.Lock()
        self._control_thread = None

    @property
    @abstractmethod
    def _node_id(self):
        """
        Return the ID of this node in the ring.
        """
        pass

    @property
    @abstractmethod
    def _ring_size(self):
        """
        Return the size of the ring (number of nodes).
        """
        pass

    @property
    @abstractmethod
    def _input_control_middleware(self):
        """
        Return the input middleware to consume control messages.
        """
        pass

    @property
    @abstractmethod
    def _output_control_middleware(self):
        """
        Return the output middleware to send control messages to the next node in the ring.
        """
        pass

    @abstractmethod
    def _ring_routing_key(self, node_id):
        """
        Return the routing key to send a message to the given node_id in the ring.
        """
        pass

    @abstractmethod
    def _flush_data(self, client_id, gateway_id):
        """
        Flush any buffered data.
        """
        pass

    @abstractmethod
    def _get_total_sent_count(self, _client_id, _gateway_id, current_total):
        """
        Get the total sent count for the given client_id and gateway_id.
        """
        pass

    @abstractmethod
    def _get_final_eof_count(self, ring_eof):
        """
        Get the final EOF count to send to the output middleware when this node is the coordinator.
        """
        pass

    def _get_total_count(self, key, count_dict, partial_dict, lock, current_total):
        """
        Get the total count for the given key by summing the count from the count_dict and the current total, and subtracting the partial count from the partial_dict.
        """
        with lock:
            count = count_dict.get(key, 0)
        partial = partial_dict.get(key, 0)
        partial_dict[key] = count
        return current_total + count - partial

    def _increment_processed_count(self, client_id, gateway_id):
        """
        Increment the processed count for the given client_id and gateway_id.
        """
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

    def _handle_control_eof_message(self, client_id, gateway_id, ring_eof):
        """
        Handle a RING_EOF message by either forwarding it to the next node in the ring, flushing data and sending an EOF to the output middleware if this node is the coordinator.
        """
        if ring_eof.coordinator_id is None:
            ring_eof = self._update_ring_eof(client_id, gateway_id, ring_eof)
        else:
            self._flush_data(client_id, gateway_id)
            if ring_eof.coordinator_id == self._node_id:
                self._output_middleware.send(
                    internal.serialize_msg(
                        internal.MsgType.EOF,
                        client_id,
                        gateway_id,
                        EOF(self._get_final_eof_count(ring_eof)),
                    )
                )
                return
        self._output_control_middleware.send(
            internal.serialize_msg(
                internal.MsgType.RING_EOF, client_id, gateway_id, ring_eof
            ),
            routing_key=self._ring_routing_key(self._get_next_node_id()),
        )

    def _handle_control_message(self, message, ack, nack):
        """
        Handle a control message by processing it as a RING_EOF message.
        """
        try:
            _, client_id, gateway_id, ring_eof = internal.deserialize_msg(message)
            self._handle_control_eof_message(client_id, gateway_id, ring_eof)
            ack()
        except Exception:
            nack()
            raise

    def _get_next_node_id(self):
        """
        Get the ID of the next node in the ring.
        """
        return (self._node_id + 1) % self._ring_size

    def _handle_eof_message(self, client_id, gateway_id, eof):
        """
        Handle an EOF message by sending a RING_EOF to the next node in the ring.
        """
        total_processed_count = self._get_total_count(
            (client_id, gateway_id),
            self._processed_counts,
            self._partial_processed_count,
            self._processed_counts_lock,
            0,
        )
        self._output_control_middleware.send(
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
            routing_key=self._ring_routing_key(self._get_next_node_id()),
        )

    def _handle_data_message(self, msg_type, client_id, gateway_id, payload):
        """
        Handle a data message by processing it and incrementing the processed count for the given client_id and gateway_id.
        """
        self._increment_processed_count(client_id, gateway_id)

    def start(self):
        """
        Start the worker and the control thread to listen for RING_EOF messages.
        """
        self._control_thread = threading.Thread(
            target=self._input_control_middleware.start_consuming,
            args=(self._handle_control_message,),
            daemon=True,
        )
        self._control_thread.start()
        super().start()

    def shutdown(self):
        """
        Shutdown the worker and stop the control exchange and the control thread.
        """
        super().shutdown()
        self._input_control_middleware.stop_consuming_threadsafe()
        if self._control_thread and self._control_thread.is_alive():
            self._control_thread.join()
