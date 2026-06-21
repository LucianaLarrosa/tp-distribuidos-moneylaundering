import os


class BatchSpill:
    def __init__(self, spill_dir, serialize, deserialize):
        self._dir = spill_dir
        self._serialize = serialize
        self._deserialize = deserialize
        self._files = {}
        os.makedirs(spill_dir, exist_ok=True)

    def _path(self, key):
        return os.path.join(self._dir, f"{key}.jsonl")

    def _open(self, key):
        f = self._files.get(key)
        if f is None:
            f = open(self._path(key), "a", encoding="utf-8")
            self._files[key] = f
        return f

    def close(self, key):
        f = self._files.pop(key, None)
        if f is not None:
            try:
                f.close()
            except Exception:
                pass

    def write(self, key, batch):
        f = self._open(key)
        f.write(self._serialize(batch))
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())

    def drain(self, key, emit):
        self.close(key)
        path = self._path(key)
        if not os.path.exists(path):
            return
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    emit(self._deserialize(line))
        os.remove(path)

    def close_all(self):
        for key in list(self._files.keys()):
            self.close(key)
