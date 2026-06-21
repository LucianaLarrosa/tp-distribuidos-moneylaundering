import os
import time

from common.persistence.state_store import StateStore

FILE_NAME = "gateway_clients.json"


class ClientRegistry:
    def __init__(self, manager, state_dir):
        self._states = manager.dict()
        self._lock = manager.Lock()
        self._store = (
            StateStore(os.path.join(state_dir, FILE_NAME)) if state_dir else None
        )

    def load(self):
        if self._store is None:
            return
        with self._lock:
            for record in self._store.load():
                self._states[record["client_id"]] = {
                    "timestamp": record["timestamp"],
                    "connected": False,
                    "gateway_id": record["gateway_id"],
                }

    def connect(self, client_id, gateway_id):
        with self._lock:
            self._states[client_id] = {
                "timestamp": time.time(),
                "connected": True,
                "gateway_id": gateway_id,
            }
            self._persist_locked()

    def touch(self, client_id):
        with self._lock:
            state = self._states.get(client_id)
            if state is None:
                return
            state["timestamp"] = time.time()
            self._states[client_id] = state

    def disconnect(self, client_id):
        with self._lock:
            state = self._states.get(client_id)
            if state is None:
                return
            state["connected"] = False
            state["timestamp"] = time.time()
            self._states[client_id] = state
            self._persist_locked()

    def remove(self, client_id):
        with self._lock:
            self._states.pop(client_id, None)
            self._persist_locked()

    def dead_clients(self, timeout):
        now = time.time()
        dead = []
        with self._lock:
            for client_id, state in self._states.items():
                if not state["connected"] and now - state["timestamp"] > timeout:
                    dead.append((client_id, state["gateway_id"]))
        return dead

    def persist(self):
        with self._lock:
            self._persist_locked()

    def _persist_locked(self):
        if self._store is None:
            return
        records = []
        for client_id, state in self._states.items():
            records.append(
                {
                    "client_id": client_id,
                    "gateway_id": state["gateway_id"],
                    "timestamp": state["timestamp"],
                }
            )
        self._store.compact(records)
