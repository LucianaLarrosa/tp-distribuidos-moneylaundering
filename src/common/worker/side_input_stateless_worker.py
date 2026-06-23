import logging
import threading
from abc import abstractmethod

from common.protocol.internal import internal
from common.models.eof import CLEANUP_EXPECTED_COUNT
from common.utils import SideInputTracker
from common.worker.stateless_worker import StatelessWorker

SIDE_CHANNEL = "side"


class SideInputStatelessWorker(StatelessWorker):

    def __init__(self, config):
        super().__init__(config)
        self._side_input = SideInputTracker()
        self._side_input_thread = None

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
    def _apply_side_delta(self, client_id, delta):
        pass

    def _on_side_input_ready(self, client_id):
        pass

    def _cleanup_state(self, client_id):
        super()._cleanup_state(client_id)
        self._side_input.drop(client_id)

    def _handle_side_message(self, message, ack, nack):
        try:
            msg_type, client_id, payload, message_id = internal.deserialize_msg(message)
            became_ready = False
            with self._state_lock:
                seen = self._seen.setdefault((SIDE_CHANNEL, client_id), set())
                if message_id in seen:
                    ack()
                    return
                if msg_type == self._side_batch_msg_type:
                    delta = self._side_delta(payload)
                    self._apply_side_delta(client_id, delta)
                    became_ready = self._side_input.track_batch(client_id)
                    record = {
                        "ch": SIDE_CHANNEL,
                        "mid": message_id,
                        "c": client_id,
                        "side": delta,
                    }
                elif msg_type == internal.MsgType.EOF:
                    if payload.message_count == CLEANUP_EXPECTED_COUNT:
                        self._cleanup_flow(client_id)
                        ack()
                        return
                    became_ready = self._side_input.set_expected(
                        client_id, payload.message_count
                    )
                    record = {
                        "ch": SIDE_CHANNEL,
                        "mid": message_id,
                        "c": client_id,
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
                self._on_side_input_ready(client_id)
        except Exception as e:
            logging.error("Error handling side-input message: %s", e)
            nack()
            raise

    def _replay_record(self, record):
        super()._replay_record(record)
        if record["ch"] == SIDE_CHANNEL:
            client_id = record["c"]
            if "side_eof" in record:
                self._side_input.set_expected(client_id, record["side_eof"])
            else:
                self._apply_side_delta(client_id, record["side"])
                self._side_input.track_batch(client_id)

    def _drain_ready_flows(self):
        for client_id in self._flow_keys():
            if self._side_input.is_ready(client_id):
                self._on_side_input_ready(client_id)

    def start(self):
        self._recover()
        self._drain_ready_flows()
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
