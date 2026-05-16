import threading

from common.worker.ring_coordinated_worker import RingCoordinatedWorker


class Sharder(RingCoordinatedWorker):
    def __init__(self) -> None:
        super().__init__()
        self._sent_counts = {}  # (client_id, gateway_id) -> sent_count
        self._sent_counts_lock = threading.Lock()

    def _handle_control_eof_message(self):
        pass

    def _handle_eof_message(self):
        pass
