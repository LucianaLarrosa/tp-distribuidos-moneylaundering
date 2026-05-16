import logging
import signal
from abc import ABC, abstractmethod

from common.protocol import internal


class Worker(ABC):
    def __init__(self):
        """
        Initialize the worker setting up signal handling for graceful shutdown.
        """
        signal.signal(signal.SIGTERM, lambda *_: self.shutdown())

    @property
    @abstractmethod
    def _input_middleware(self):
        """
        Return the input middleware to consume messages from the previous stage.
        """
        pass

    @property
    @abstractmethod
    def _output_middleware(self):
        """
        Return the output middleware to forward messages to the next stage.
        """
        pass

    @abstractmethod
    def _handle_eof_message(self, client_id, gateway_id, eof):
        """
        Handle the EOF message for the given client_id and gateway_id.
        """
        pass

    @abstractmethod
    def _handle_data_message(self, msg_type, client_id, gateway_id, payload):
        """
        Handle a data message for the given client_id and gateway_id.
        """
        pass

    def _handle_message(self, message, ack, nack):
        """
        Handle incoming messages from the input middleware accordingly.
        """
        try:
            msg_type, client_id, gateway_id, payload = internal.deserialize_msg(message)
            if msg_type == internal.MsgType.EOF:
                self._handle_eof_message(client_id, gateway_id, payload)
            else:
                self._handle_data_message(msg_type, client_id, gateway_id, payload)
            ack()
        except Exception as e:
            logging.error(f"Error handling message: {e}")
            nack()
            raise

    def start(self):
        """
        Start the worker by consuming messages from the input middleware.
        """
        logging.info("Starting worker...")
        self._input_middleware.start_consuming(self._handle_message)

    def shutdown(self):
        """
        Shutdown the worker by stopping the input middleware and closing connections.
        """
        logging.info("Shutting down worker...")
        self._input_middleware.stop_consuming()
        self._input_middleware.close()
        self._output_middleware.close()
