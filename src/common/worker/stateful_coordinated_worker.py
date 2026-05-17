from common.worker.ring_coordinated_worker import RingCoordinatedWorker


class StatefulCoordinatedWorker(RingCoordinatedWorker):
    def _get_total_sent_count(self, _client_id, _gateway_id, current_total):
        """
        Get the total sent count for the given client_id and gateway_id. A stateful-coordinated worker does not track sent counts, so this method simply returns the current total sent count from the RING_EOF message.
        """
        return current_total

    def _get_final_eof_count(self, _ring_eof):
        """
        Get the final EOF count to send to the output middleware when this node is the coordinator. In a stateful-coordinated worker, the final EOF count is the ring size.
        """
        return self._ring_size
