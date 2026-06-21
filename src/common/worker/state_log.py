import logging
import os
import threading

from common.persistence.state_store import StateStore

SNAPSHOT = "snap"
CLEANUP = "cleanup"


class StateLog:
    _COMPACT_THRESHOLD = 10000

    def __init__(self):
        self._state_lock = threading.Lock()
        self._seen = {}  # (channel, client_id, gateway_id) -> set of message_id
        self._recovered = False
        self._appends_since_compact = 0
        state_dir = os.environ.get("STATE_DIR")
        self._state_store = (
            StateStore(os.path.join(state_dir, "state.wal")) if state_dir else None
        )

    def _apply_delta(self, client_id, gateway_id, delta):
        raise NotImplementedError

    def _state_as_delta(self, client_id, gateway_id):
        return None

    def _cleanup_state(self, client_id, gateway_id):
        pass

    def _forget_flow(self, client_id, gateway_id):
        stale = set()
        for key in self._seen:
            if key[1:] == (client_id, gateway_id):
                stale.add(key)
        for key in stale:
            del self._seen[key]
        self._cleanup_state(client_id, gateway_id)

    def _cleanup_flow(self, client_id, gateway_id):
        logging.info("[cleanup] dropping flow %s:%s", client_id, gateway_id)
        self._forget_flow(client_id, gateway_id)
        if self._state_store is not None:
            self._state_store.append(
                {"ch": CLEANUP, "c": client_id, "g": gateway_id}
            )

    def _flow_keys(self):
        return {(c, g) for (_, c, g) in self._seen}

    def _snapshot_flow(self, client_id, gateway_id):
        seen = {}
        for (ch, c, g), mids in self._seen.items():
            if (c, g) == (client_id, gateway_id):
                seen[ch] = list(mids)
        return {
            "ch": SNAPSHOT,
            "c": client_id,
            "g": gateway_id,
            "seen": seen,
            "delta": self._state_as_delta(client_id, gateway_id),
        }

    def _restore_snapshot(self, record):
        client_id, gateway_id = record["c"], record["g"]
        for ch, mids in record["seen"].items():
            self._seen.setdefault((ch, client_id, gateway_id), set()).update(mids)
        if record.get("delta") is not None:
            self._apply_delta(client_id, gateway_id, record["delta"])

    def _compact(self):
        if self._state_store is None:
            return
        records = [self._snapshot_flow(c, g) for (c, g) in self._flow_keys()]
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
        if record["ch"] == CLEANUP:
            self._forget_flow(record["c"], record["g"])
            return
        mid = record.get("mid")
        if mid is not None:
            self._seen.setdefault((record["ch"], record["c"], record["g"]), set()).add(
                mid
            )
        if record.get("delta") is not None:
            self._apply_delta(record["c"], record["g"], record["delta"])

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
