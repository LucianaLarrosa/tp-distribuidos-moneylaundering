from common.models.eof import is_cleanup_eof
from common.worker.worker import Worker


class StatelessWorker(Worker):
    def _handle_eof_message(self, client_id, eof):
        """
        Handle an EOF message by forwarding it to the next stage.
        """
        if is_cleanup_eof(eof):
            self._cleanup_flow(client_id)
        self._send_final_eof(client_id, eof)
