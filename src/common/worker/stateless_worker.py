from common.worker.worker import Worker


class StatelessWorker(Worker):
    def _handle_eof_message(self, client_id, eof):
        """
        Handle an EOF message by forwarding it to the next stage.
        """
        self._send_final_eof(client_id, eof)
