import threading

from common.worker.ring_coordinated_worker import RingCoordinatedWorker


class SentCoordinatedWorker(RingCoordinatedWorker):
    def __init__(self):
        """
        Initialize the sent-coordinated worker with a dictionary to track sent counts and a lock to synchronize access to this dictionary.
        """
        super().__init__()
        self._sent_count = (
            {}
        )  # (client_id, gateway_id) -> sent_count | actual total sent count
        self._partial_sent_count = (
            {}
        )  # (client_id, gateway_id) -> partial_sent_count | previous sent count sent to ring
        self._sent_count_lock = threading.Lock()

    def _get_total_sent_count(self, client_id, gateway_id, current_total):
        """
        Get the total sent count for the given client_id and gateway_id. A sent-coordinated worker tracks sent counts, so this method returns the calculated total sent count.
        """
        return self._get_total_count(
            (client_id, gateway_id),
            self._sent_count,
            self._partial_sent_count,
            self._sent_count_lock,
            current_total,
        )

    def _get_final_eof_count(self, ring_eof):
        """
        Get the final EOF count to send to the output middleware when this node is the coordinator. In a sent-coordinated worker, the final EOF count is the total sent count from the RING_EOF message.
        """
        return ring_eof.total_sent_count

    def _increment_sent_count(self, client_id, gateway_id):
        """
        Increment the sent count for the given client_id and gateway_id.
        """
        with self._sent_count_lock:
            self._sent_count[(client_id, gateway_id)] = (
                self._sent_count.get((client_id, gateway_id), 0) + 1
            )
