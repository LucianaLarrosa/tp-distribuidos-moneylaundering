import logging
import threading
from abc import abstractmethod

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.protocol import internal
from common.protocol.internal import internal
from common.utils import SideInputTracker
from common.worker.ring_coordinated_worker import RingCoordinatedWorker


class SideInputStatelessCoordinatedWorker(RingCoordinatedWorker):
    def __init__(self):
        super().__init__()
        self._side_input = SideInputTracker()
        self._side_input_thread = None
        self._side_input_ready = {}
        self._deferred_data_eofs = {}
        self._deferred_ring_eofs = {}
        self._deferred_lock = threading.Lock()
        self._side_output_control_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=self._rabbitmq_host,
            exchange_name=self._control_exchange_name,
            routing_keys=[],
        )

    @property
    @abstractmethod
    def _input_side_middleware(self):
        pass

    @property
    @abstractmethod
    def _side_batch_msg_type(self):
        pass

    @abstractmethod
    def _process_side_batch(self, client_id, gateway_id, payload):
        pass

    def _on_side_input_ready(self, client_id, gateway_id):
        pass

    def _get_total_sent_count(self, _client_id, _gateway_id, current_total):
        return current_total

    def _get_final_eof_count(self, ring_eof):
        return ring_eof.total_processed_count

    def _handle_side_message(self, message, ack, nack):
        try:
            msg_type, client_id, gateway_id, payload = internal.deserialize_msg(message)
            key = (client_id, gateway_id)

            if msg_type == self._side_batch_msg_type:
                self._process_side_batch(client_id, gateway_id, payload)
                became_ready = self._side_input.track_batch(key)
                ack()
                if became_ready:
                    self._mark_side_input_ready(client_id, gateway_id)
                return

            if msg_type == internal.MsgType.EOF:
                logging.info(
                    "Side-input EOF for %s: expected=%s", key, payload.message_count
                )
                became_ready = self._side_input.set_expected(key, payload.message_count)
                ack()
                if became_ready:
                    self._mark_side_input_ready(client_id, gateway_id)
                return

            logging.warning("Unexpected side-input message type: %s", msg_type)
            nack()
        except Exception as e:
            logging.error("Error handling side-input message: %s", e)
            nack()
            raise

    def _handle_eof_message(self, client_id, gateway_id, eof):
        key = (client_id, gateway_id)
        with self._deferred_lock:
            if not self._side_input_ready.get(key, False):
                self._deferred_data_eofs[key] = eof
                return
        super()._handle_eof_message(client_id, gateway_id, eof)

    def _handle_control_eof_message(self, client_id, gateway_id, ring_eof):
        key = (client_id, gateway_id)
        with self._deferred_lock:
            if not self._side_input_ready.get(key, False):
                self._deferred_ring_eofs[key] = ring_eof
                return
        super()._handle_control_eof_message(client_id, gateway_id, ring_eof)

    def _mark_side_input_ready(self, client_id, gateway_id):
        key = (client_id, gateway_id)
        with self._deferred_lock:
            self._side_input_ready[key] = True
            deferred_data = self._deferred_data_eofs.pop(key, None)
            deferred_ring = self._deferred_ring_eofs.pop(key, None)
        self._on_side_input_ready(client_id, gateway_id)
        if deferred_data is not None:
            super()._handle_eof_message(
                client_id,
                gateway_id,
                deferred_data,
                self._side_output_control_exchange,
            )
        if deferred_ring is not None:
            super()._handle_control_eof_message(
                client_id,
                gateway_id,
                deferred_ring,
                self._side_output_control_exchange,
            )

    def start(self):
        self._side_input_thread = threading.Thread(
            target=self._input_side_middleware.start_consuming,
            args=(self._handle_side_message,),
            daemon=True,
        )
        self._side_input_thread.start()
        super().start()

    def shutdown(self):
        super().shutdown()
        if self._side_input_thread and self._side_input_thread.is_alive():
            self._input_side_middleware.stop_consuming_threadsafe()
            self._side_input_thread.join()
        self._input_side_middleware.close()
        self._side_output_control_exchange.close()
