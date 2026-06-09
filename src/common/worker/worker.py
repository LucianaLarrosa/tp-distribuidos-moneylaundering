import logging
import signal
from abc import ABC, abstractmethod

from common.protocol.internal import internal


class Worker(ABC):
    def __init__(self):
        self._closed = False
        self._current_message_id = ""
        signal.signal(signal.SIGTERM, lambda *_: self.shutdown())

    @property
    @abstractmethod
    def _input_middleware(self):
        pass

    @property
    @abstractmethod
    def _output_middleware(self):
        pass

    @abstractmethod
    def _handle_eof_message(self, client_id, gateway_id, eof):
        pass

    @abstractmethod
    def _handle_data_message(self, msg_type, client_id, gateway_id, payload):
        pass

    @abstractmethod
    def _send_final_eof(self, client_id, gateway_id, eof):
        pass

    def _handle_message(self, message, ack, nack):
        """
        Handle incoming messages from the input middleware accordingly.
        """
        try:
            msg_type, client_id, gateway_id, payload, message_id = (
                internal.deserialize_msg(message)
            )
            self._current_message_id = message_id
            if msg_type == internal.MsgType.EOF:
                self._handle_eof_message(client_id, gateway_id, payload)
            else:
                self._handle_data_message(msg_type, client_id, gateway_id, payload)
            ack()
        except Exception as e:
            logging.error(f"Error handling message: {e}")
            nack()
            raise

    def _send(
        self,
        out_middleware,
        msg_type,
        client_id,
        gateway_id,
        payload,
        routing_key=None,
        message_id=None,
    ):
        if message_id is None:
            message_id = self._current_message_id
        msg = internal.serialize_msg(
            msg_type, client_id, gateway_id, payload, message_id=message_id
        )
        if routing_key is not None:
            out_middleware.send(msg, routing_key=routing_key)
        else:
            out_middleware.send(msg)

    def start(self):
        logging.info("Starting worker...")
        self._input_middleware.start_consuming(self._handle_message)

    def shutdown(self):
        if self._closed:
            return
        self._closed = True
        logging.info("Shutting down worker...")
        self._input_middleware.stop_consuming()
        self._input_middleware.close()
        self._output_middleware.close()
