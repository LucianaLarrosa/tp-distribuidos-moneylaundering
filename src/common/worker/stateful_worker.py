import threading
from abc import abstractmethod

from common.worker.worker import Worker
from common.protocol import internal
from common.models.eof import EOF, RingEOF


class StatefulWorker(Worker):
    def __init__(self) -> None:
        """
        Initialize the stateful worker with a dictionary to track processed counts and a lock to synchronize access to this dictionary. Also initialize a control thread to listen for RING_EOF messages.
        """
        super().__init__()
        self._processed_counts = {}  # (client_id, gateway_id) -> processed_count
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
    def _control_exchange(self):
        """
        Return the control exchange to send and receive control messages (RING_EOF).
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

    def _update_ring_eof(self, client_id, gateway_id, ring_eof):
        """
        Resolve the RingEOF by updating the processed count and determining the coordinator id.
        """
        with self._processed_counts_lock:
            processed_count = self._processed_counts.get((client_id, gateway_id), 0)
        total_processed_count = ring_eof.total_processed_count + processed_count
        coordinator_id = (
            self._node_id if total_processed_count >= ring_eof.expected_count else None
        )
        return RingEOF(
            ring_eof.expected_count,
            total_processed_count,
            coordinator_id,
        )

    def _handle_control_eof_message(self, client_id, gateway_id, ring_eof):
        """
        Handle a RING_EOF message by either forwarding it to the next node in the ring or sending an EOF to the output middleware if this node is the coordinator.
        """
        if ring_eof.coordinator_id is None:
            ring_eof = self._update_ring_eof(client_id, gateway_id, ring_eof)
        elif ring_eof.coordinator_id == self._node_id:
            self._output_middleware.send(
                internal.serialize_msg(
                    internal.MsgType.EOF,
                    client_id,
                    gateway_id,
                    EOF(message_count=ring_eof.expected_count),
                )
            )
            return
        else:
            self._flush_data(client_id, gateway_id)
        self._control_exchange.send(
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

    def _handle_eof_message(self, client_id, gateway_id, message_count):
        """
        Handle an EOF message by sending a RING_EOF to the next node in the ring.
        """
        with self._processed_counts_lock:
            processed_count = self._processed_counts.get((client_id, gateway_id), 0)
        self._control_exchange.send(
            internal.serialize_msg(
                internal.MsgType.RING_EOF,
                client_id,
                gateway_id,
                RingEOF(
                    expected_count=message_count,
                    total_processed_count=processed_count,
                ),
            ),
            routing_key=self._ring_routing_key(self._get_next_node_id()),
        )

    def start(self):
        """
        Start the worker and the control thread to listen for RING_EOF messages.
        """
        self._control_thread = threading.Thread(
            target=self._control_exchange.start_consuming,
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
        self._control_exchange.stop_consuming_threadsafe()
        if self._control_thread and self._control_thread.is_alive():
            self._control_thread.join()
