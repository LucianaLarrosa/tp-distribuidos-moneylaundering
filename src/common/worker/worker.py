import logging
import signal
from abc import ABC, abstractmethod

from common.health import HealthResponder
from common.communication.protocol import internal
from common.idempotency.state_log import StateLog

MAIN_CHANNEL = "main"


class Worker(StateLog, ABC):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self._closed = False
        self._current_message_id = ""
        signal.signal(signal.SIGTERM, lambda *_: self.stop())
        self._health_responder = HealthResponder(
            config.node_name, config.ping_port, config.ping_pong_host
        )
        self._health_responder.start()

    @property
    @abstractmethod
    def _input_middleware(self):
        pass

    @property
    @abstractmethod
    def _output_middleware(self):
        pass

    @abstractmethod
    def _handle_eof_message(self, client_id, eof):
        pass

    @abstractmethod
    def _handle_data_message(self, msg_type, client_id, payload):
        pass

    @abstractmethod
    def _send_final_eof(self, client_id, eof):
        pass

    def _handle_message(self, message, ack, nack):
        try:
            msg_type, client_id, payload, message_id = internal.deserialize_msg(message)
            self._current_message_id = message_id
            if self._state_store is None:
                if msg_type == internal.MsgType.EOF:
                    self._handle_eof_message(client_id, payload)
                else:
                    self._handle_data_message(msg_type, client_id, payload)
                ack()
                return
            with self._state_lock:
                seen = self._seen.setdefault((MAIN_CHANNEL, client_id), set())
                if message_id in seen:
                    ack()
                    return
                if msg_type == internal.MsgType.EOF:
                    self._handle_eof_message(client_id, payload)
                    delta = None
                else:
                    delta = self._handle_data_message(msg_type, client_id, payload)
                seen.add(message_id)
                self._state_store.append(
                    {
                        "ch": MAIN_CHANNEL,
                        "mid": message_id,
                        "c": client_id,
                        "delta": delta,
                        "eof": msg_type == internal.MsgType.EOF,
                    }
                )
                self._note_append()
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
        payload,
        routing_key=None,
        message_id=None,
    ):
        if message_id is None:
            message_id = self._current_message_id
        msg = internal.serialize_msg(
            msg_type, client_id, payload, message_id=message_id
        )
        if routing_key is not None:
            out_middleware.send(msg, routing_key=routing_key)
        else:
            out_middleware.send(msg)

    def start(self):
        logging.info("Starting worker...")
        self._recover()
        self._input_middleware.start_consuming(self._handle_message)

    def stop(self):
        self._input_middleware.stop_consuming_threadsafe()

    def shutdown(self):
        if self._closed:
            return
        self._closed = True
        logging.info("Shutting down worker...")
        self._health_responder.stop()
        self._input_middleware.stop_consuming()
        self._input_middleware.close()
        self._output_middleware.close()
        if self._state_store is not None:
            self._state_store.close()
