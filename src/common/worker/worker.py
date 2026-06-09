import logging
import signal
from abc import ABC, abstractmethod

from common.health import HeartbeatSender
from common.protocol.internal import internal


class Worker(ABC):
    def __init__(self, config):
        self.config = config
        self._closed = False
        signal.signal(signal.SIGTERM, lambda *_: self.shutdown())
        self._heartbeat = HeartbeatSender(
            config.node_name,
            config.watchdog_host,
            config.watchdog_port,
            config.heartbeat_interval_seconds,
        )
        self._heartbeat.start()

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
        logging.info("Starting worker...")
        self._input_middleware.start_consuming(self._handle_message)

    def shutdown(self):
        if self._closed:
            return
        self._closed = True
        logging.info("Shutting down worker...")
        self._heartbeat.stop()
        self._input_middleware.stop_consuming()
        self._input_middleware.close()
        self._output_middleware.close()
