import logging
import threading
from abc import abstractmethod

from common.communication.protocol import internal
from common.models.eof import CLEANUP_EXPECTED_COUNT
from common.worker.utils.side_input_tracker import SideInputTracker
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

    def _side_state_as_delta(self, client_id):
        return None

    def _snapshot_flow(self, client_id):
        record = super()._snapshot_flow(client_id)
        received, expected, _ = self._side_input.stats(client_id)
        record["side_received"] = received
        record["side_expected"] = expected
        record["side_state"] = self._side_state_as_delta(client_id)
        return record

    def _restore_snapshot(self, record):
        super()._restore_snapshot(record)
        client_id = record["c"]
        side_state = record.get("side_state")
        if side_state is not None:
            self._apply_side_delta(client_id, side_state)
        if "side_expected" in record or "side_received" in record:
            self._side_input.restore(
                client_id,
                record.get("side_received", 0),
                record.get("side_expected"),
            )

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
