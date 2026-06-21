import logging
import os
import signal
import threading
from abc import ABC, abstractmethod

from common.health import HealthResponder
from common.persistence.state_store import StateStore
from common.protocol.internal import internal

MAIN_CHANNEL = "main"
CONTROL_CHANNEL = "control"
SIDE_CHANNEL = "side"
SNAPSHOT = "snap"


class Worker(ABC):
    _COMPACT_THRESHOLD = 10000

    def __init__(self, config):
        self.config = config
        self._closed = False
        self._current_message_id = ""
        self._state_lock = threading.Lock()
        self._seen = {}  # (channel, client_id) -> set of message_id
        self._recovered = False
        self._appends_since_compact = 0
        state_dir = os.environ.get("STATE_DIR")
        self._state_store = (
            StateStore(os.path.join(state_dir, "state.wal")) if state_dir else None
        )
        signal.signal(signal.SIGTERM, lambda *_: self.shutdown())
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

    def _apply_delta(self, client_id, delta):
        raise NotImplementedError

    def _state_as_delta(self, client_id):
        return None

    def _flow_keys(self):
        return {c for (_, c) in self._seen}

    def _snapshot_flow(self, client_id):
        seen = {}
        for (ch, c), mids in self._seen.items():
            if c == client_id:
                seen[ch] = list(mids)
        return {
            "ch": SNAPSHOT,
            "c": client_id,
            "seen": seen,
            "delta": self._state_as_delta(client_id),
        }

    def _restore_snapshot(self, record):
        client_id = record["c"]
        for ch, mids in record["seen"].items():
            self._seen.setdefault((ch, client_id), set()).update(mids)
        if record.get("delta") is not None:
            self._apply_delta(client_id, record["delta"])

    def _compact(self):
        if self._state_store is None:
            return
        records = [self._snapshot_flow(c) for c in self._flow_keys()]
        self._state_store.compact(records)
        self._appends_since_compact = 0

    def _note_append(self):
        self._appends_since_compact += 1
        if self._appends_since_compact >= self._COMPACT_THRESHOLD:
            self._appends_since_compact = 0
            self._compact()

    def _replay_record(self, record):
        if record["ch"] == SNAPSHOT:
            self._restore_snapshot(record)
            return
        mid = record.get("mid")
        if mid is not None:
            self._seen.setdefault((record["ch"], record["c"]), set()).add(mid)
        if record.get("delta") is not None:
            self._apply_delta(record["c"], record["delta"])

    def _recover(self):
        if self._state_store is None or self._recovered:
            return
        self._recovered = True
        replayed = 0
        for record in self._state_store.load():
            self._replay_record(record)
            replayed += 1
        if replayed:
            logging.info(f"Recovered {replayed} records from state WAL")
            self._compact()

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
