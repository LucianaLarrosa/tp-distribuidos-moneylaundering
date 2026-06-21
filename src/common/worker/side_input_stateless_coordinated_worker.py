import logging
import threading
from abc import abstractmethod

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.protocol.internal import internal
from common.models.eof import EOF, RingEOF, CLEANUP_EXPECTED_COUNT
from common.utils import SideInputTracker
from common.worker.ring_coordinated_worker import RingCoordinatedWorker

SIDE_CHANNEL = "side"
DEFER_CHANNEL = "defer"
UNDEFER_CHANNEL = "undefer"


class SideInputStatelessCoordinatedWorker(RingCoordinatedWorker):
    def __init__(self, config):
        super().__init__(config)
        self._side_input = SideInputTracker()
        self._side_input_thread = None
        self._deferred_data_eofs = {}
        self._deferred_ring_eofs = {}
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
    def _side_delta(self, payload):
        pass

    @abstractmethod
    def _apply_side_delta(self, client_id, gateway_id, delta):
        pass

    def _on_side_input_ready(self, client_id, gateway_id):
        pass

    def _compact(self):
        pass

    def _get_total_sent_count(self, _client_id, _gateway_id, current_total):
        return current_total

    def _get_final_eof_count(self, ring_eof):
        return ring_eof.total_processed_count

    def _cleanup_state(self, client_id, gateway_id):
        super()._cleanup_state(client_id, gateway_id)
        key = (client_id, gateway_id)
        self._deferred_data_eofs.pop(key, None)
        self._deferred_ring_eofs.pop(key, None)
        self._side_input.drop(key)

    def _handle_side_message(self, message, ack, nack):
        try:
            msg_type, client_id, gateway_id, payload, message_id = (
                internal.deserialize_msg(message)
            )
            key = (client_id, gateway_id)
            became_ready = False
            with self._state_lock:
                seen = self._seen.setdefault(
                    (SIDE_CHANNEL, client_id, gateway_id), set()
                )
                if message_id in seen:
                    ack()
                    return
                if msg_type == self._side_batch_msg_type:
                    delta = self._side_delta(payload)
                    self._apply_side_delta(client_id, gateway_id, delta)
                    became_ready = self._side_input.track_batch(key)
                    record = {
                        "ch": SIDE_CHANNEL,
                        "mid": message_id,
                        "c": client_id,
                        "g": gateway_id,
                        "side": delta,
                    }
                elif msg_type == internal.MsgType.EOF:
                    if payload.message_count == CLEANUP_EXPECTED_COUNT:
                        self._cleanup_flow(client_id, gateway_id)
                        ack()
                        return
                    became_ready = self._side_input.set_expected(
                        key, payload.message_count
                    )
                    record = {
                        "ch": SIDE_CHANNEL,
                        "mid": message_id,
                        "c": client_id,
                        "g": gateway_id,
                        "side_eof": payload.message_count,
                    }
                else:
                    logging.warning("Unexpected side-input message type: %s", msg_type)
                    nack()
                    return
                seen.add(message_id)
                self._state_store.append(record)
            ack()
            if became_ready:
                self._mark_side_input_ready(client_id, gateway_id)
        except Exception as e:
            logging.error("Error handling side-input message: %s", e)
            nack()
            raise

    def _handle_eof_message(self, client_id, gateway_id, eof):
        key = (client_id, gateway_id)
        cleanup = eof.message_count == CLEANUP_EXPECTED_COUNT
        if not cleanup and not self._side_input.is_ready(key):
            self._deferred_data_eofs[key] = eof
            self._state_store.append(
                {
                    "ch": DEFER_CHANNEL,
                    "c": client_id,
                    "g": gateway_id,
                    "kind": "data_eof",
                    "count": eof.message_count,
                }
            )
            return
        super()._handle_eof_message(client_id, gateway_id, eof)

    def _handle_control_eof_message(
        self, client_id, gateway_id, ring_eof, in_message_id="", output_exchange=None
    ):
        key = (client_id, gateway_id)
        cleanup = ring_eof.expected_count == CLEANUP_EXPECTED_COUNT
        if not cleanup and not self._side_input.is_ready(key):
            self._deferred_ring_eofs[key] = (ring_eof, in_message_id)
            self._state_store.append(
                {
                    "ch": DEFER_CHANNEL,
                    "c": client_id,
                    "g": gateway_id,
                    "kind": "ring",
                    "ring_eof": {
                        "expected_count": ring_eof.expected_count,
                        "total_processed_count": ring_eof.total_processed_count,
                        "coordinator_id": ring_eof.coordinator_id,
                        "total_sent_count": ring_eof.total_sent_count,
                    },
                    "in_mid": in_message_id,
                }
            )
            return
        super()._handle_control_eof_message(
            client_id, gateway_id, ring_eof, in_message_id, output_exchange
        )

    def _mark_side_input_ready(self, client_id, gateway_id):
        self._on_side_input_ready(client_id, gateway_id)
        self._flush_deferred(client_id, gateway_id)

    def _flush_deferred(self, client_id, gateway_id):
        key = (client_id, gateway_id)
        with self._state_lock:
            eof = self._deferred_data_eofs.pop(key, None)
            deferred_ring = self._deferred_ring_eofs.pop(key, None)
            if eof is None and deferred_ring is None:
                return
            if eof is not None:
                super()._handle_eof_message(
                    client_id, gateway_id, eof, self._side_output_control_exchange
                )
            if deferred_ring is not None:
                ring_eof, in_message_id = deferred_ring
                super()._handle_control_eof_message(
                    client_id,
                    gateway_id,
                    ring_eof,
                    in_message_id,
                    self._side_output_control_exchange,
                )
            self._state_store.append(
                {
                    "ch": UNDEFER_CHANNEL,
                    "c": client_id,
                    "g": gateway_id,
                    "ring": self._control_state_snapshot(client_id, gateway_id),
                }
            )

    def _replay_record(self, record):
        super()._replay_record(record)
        ch = record["ch"]
        key = (record["c"], record["g"])
        if ch == SIDE_CHANNEL:
            if "side_eof" in record:
                self._side_input.set_expected(key, record["side_eof"])
            else:
                self._apply_side_delta(record["c"], record["g"], record["side"])
                self._side_input.track_batch(key)
        elif ch == DEFER_CHANNEL:
            if record["kind"] == "data_eof":
                self._deferred_data_eofs[key] = EOF(record["count"])
            else:
                self._deferred_ring_eofs[key] = (
                    RingEOF(**record["ring_eof"]),
                    record["in_mid"],
                )
        elif ch == UNDEFER_CHANNEL:
            self._deferred_data_eofs.pop(key, None)
            self._deferred_ring_eofs.pop(key, None)
            self._restore_control_state(record["c"], record["g"], record["ring"])

    def _reprocess_ready_deferred(self):
        keys = set(self._deferred_data_eofs) | set(self._deferred_ring_eofs)
        for client_id, gateway_id in keys:
            if self._side_input.is_ready((client_id, gateway_id)):
                self._on_side_input_ready(client_id, gateway_id)
                self._flush_deferred(client_id, gateway_id)

    def start(self):
        self._recover()
        self._reprocess_ready_deferred()
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
