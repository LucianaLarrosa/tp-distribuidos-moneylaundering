import threading


class SideInputTracker:
    def __init__(self):
        self._received = {}
        self._expected = {}
        self._ready = {}
        self._lock = threading.Lock()

    def track_batch(self, key):
        with self._lock:
            self._received[key] = self._received.get(key, 0) + 1
            return self._check_ready(key)

    def set_expected(self, key, expected):
        with self._lock:
            self._expected[key] = expected
            return self._check_ready(key)

    def is_ready(self, key):
        event = self._ready.get(key)
        return event is not None and event.is_set()

    def stats(self, key):
        with self._lock:
            return (
                self._received.get(key, 0),
                self._expected.get(key),
                self.is_ready(key),
            )

    def drop(self, key):
        with self._lock:
            self._received.pop(key, None)
            self._expected.pop(key, None)
            self._ready.pop(key, None)

    def _check_ready(self, key):
        event = self._ready.get(key)
        if event is None:
            event = threading.Event()
            self._ready[key] = event
        if event.is_set():
            return False
        expected = self._expected.get(key)
        if expected is None:
            return False
        if self._received.get(key, 0) < expected:
            return False
        event.set()
        return True
