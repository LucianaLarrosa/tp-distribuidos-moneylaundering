import threading

from common.worker.ring_coordinated_worker import RingCoordinatedWorker
from common.protocol import internal
from common.models.eof import EOF, RingEOF


class SentCoordinatedWorker(RingCoordinatedWorker):
    def __init__(self) -> None:
        super().__init__()
        self._message_sent_count = {}  # (client_id, gateway_id) -> sent_count
        self._partial_sent_to_ring = (
            {}
        )  # (client_id, gateway_id) -> sent_count_sent_to_ring
        self._message_sent_count_lock = threading.Lock()

    def _increment_sent_count(self, client_id, gateway_id):
        """
        Increment the sent count for the given client_id and gateway_id.
        """
        with self._message_sent_count_lock:
            self._message_sent_count[(client_id, gateway_id)] = (
                self._message_sent_count.get((client_id, gateway_id), 0) + 1
            )

    def _update_ring_eof(self, client_id, gateway_id, ring_eof):
        ring_eof_message = super()._update_ring_eof(client_id, gateway_id, ring_eof)
        return self._update_sent_ring_eof(client_id, gateway_id, ring_eof_message)

    def _handle_control_eof_message(self, client_id, gateway_id, ring_eof):
        """
        Handle a RING_EOF message by either forwarding it to the next node in the ring, flushing data and sending an EOF to the output middleware if this node is the coordinator.
        """
        if ring_eof.coordinator_id is None:
            ring_eof = self._update_ring_eof(client_id, gateway_id, ring_eof)
            self._output_control_middleware.send(
                internal.serialize_msg(
                    internal.MsgType.RING_EOF, client_id, gateway_id, ring_eof
                ),
                routing_key=self._ring_routing_key(self._get_next_node_id()),
            )
        else:
            self._flush_data(client_id, gateway_id)
            ring_eof = self._update_sent_ring_eof(client_id, gateway_id, ring_eof)
            if ring_eof.coordinator_id == self._node_id:
                self._output_middleware.send(
                    internal.serialize_msg(
                        internal.MsgType.EOF,
                        client_id,
                        gateway_id,
                        EOF(ring_eof.total_sent_count),
                    )
                )
                return
        self._output_control_middleware.send(
            internal.serialize_msg(
                internal.MsgType.RING_EOF, client_id, gateway_id, ring_eof
            ),
            routing_key=self._ring_routing_key(self._get_next_node_id()),
        )

    def _handle_eof_message(self, client_id, gateway_id, eof):
        with self._processed_counts_lock:
            processed_count = self._processed_counts.get((client_id, gateway_id), 0)
        with self._message_sent_count_lock:
            sent_count = self._message_sent_count.get((client_id, gateway_id), 0)
        self._output_control_middleware.send(
            internal.serialize_msg(
                internal.MsgType.RING_EOF,
                client_id,
                gateway_id,
                RingEOF(
                    expected_count=eof.message_count,
                    total_processed_count=processed_count,
                    total_sent_count=sent_count,
                ),
            ),
            routing_key=self._ring_routing_key(self._get_next_node_id()),
        )
        self._partial_processed_count[(client_id, gateway_id)] = processed_count
        self._partial_sent_to_ring[(client_id, gateway_id)] = sent_count

    def _update_sent_ring_eof(self, client_id, gateway_id, ring_eof):
        with self._message_sent_count_lock:
            sent_count = self._message_sent_count.get((client_id, gateway_id), 0)
            partial_sent_count_to_ring = self._partial_sent_to_ring.get(
                (client_id, gateway_id), 0
            )

            total_sent_count = (
                ring_eof.total_sent_count + sent_count - partial_sent_count_to_ring
            )
            self._partial_sent_to_ring[(client_id, gateway_id)] = sent_count
        ring_eof.total_sent_count = total_sent_count
        return ring_eof
