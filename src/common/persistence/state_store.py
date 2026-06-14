import json
import os


class StateStore:
    def __init__(self, path):
        self._path = path
        self._file = None
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)

    def load(self):
        if not os.path.exists(self._path):
            return
        with open(self._path, "rb") as f:
            for raw in f:
                yield json.loads(raw.decode("utf-8"))

    def append(self, record):
        if self._file is None:
            self._file = open(self._path, "ab")
        self._file.write(f"{json.dumps(record)}\n".encode("utf-8"))
        self._file.flush()
        os.fsync(self._file.fileno())

    def compact(self, records):
        tmp = f"{self._path}.tmp"
        with open(tmp, "wb") as f:
            for record in records:
                f.write(f"{json.dumps(record)}\n".encode("utf-8"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._path)
        parent = os.path.dirname(self._path) or "."
        dir_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        if self._file is not None:
            self._file.close()
            self._file = None

    def close(self):
        if self._file is not None:
            self._file.close()
            self._file = None
