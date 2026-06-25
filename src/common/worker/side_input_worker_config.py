import os

from common.worker.worker_config import WorkerConfig


class SideInputWorkerConfig(WorkerConfig):
    def __init__(self):
        super().__init__()
        self.node_id = int(os.environ["NODE_ID"])
