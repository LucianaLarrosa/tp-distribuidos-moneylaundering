from abc import abstractmethod

from common.worker.worker import Worker
from common.protocol import internal


class StatelessWorker(Worker):
    @abstractmethod
    def _send_eof(self, message):
        """
        Send an EOF message to the next stage.
        """
        pass

    def _handle_eof_message(self, client_id, gateway_id, message_count):
        """
        Handle an EOF message by forwarding it to the next stage.
        """
        self._send_eof(
            internal.serialize_msg(
                internal.MsgType.EOF, client_id, gateway_id, message_count
            )
        )
