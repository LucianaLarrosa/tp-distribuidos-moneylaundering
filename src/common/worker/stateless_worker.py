from abc import abstractmethod

from common.worker.worker import Worker
from common.protocol import internal


class StatelessWorker(Worker):
    @property
    @abstractmethod
    def _output_middleware(self):
        """
        Return the output middleware to forward the EOF to the next stage.
        """
        ...

    def _handle_eof_message(self, client_id, gateway_id, message_count):
        """
        Handle an EOF message by forwarding it to the next stage.
        """
        self._output_middleware.send(
            internal.serialize_msg(
                internal.MsgType.EOF, client_id, gateway_id, message_count
            )
        )
