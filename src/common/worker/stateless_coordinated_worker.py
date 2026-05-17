from common.worker.ring_coordinated_worker import RingCoordinatedWorker


class StatelessCoordinatedWorker(RingCoordinatedWorker):
    def _flush_data(self, _client_id, _gateway_id):
        """
        Flush any buffered data. In a stateless-coordinated worker, there is no data to flush.
        """
        return

    def _get_total_sent_count(self, _client_id, _gateway_id, current_total):
        """
        Get the total sent count for the given client_id and gateway_id. A stateless-coordinated worker does not track sent counts, so this method simply returns the current total sent count from the RING_EOF message.
        """
        return current_total

    def _get_final_eof_count(self, ring_eof):
        """
        Get the final EOF count to send to the output middleware when this node is the coordinator. In a stateless-coordinated worker, the final EOF count is simply the total processed count from the ring EOF message.
        """
        return ring_eof.total_processed_count
