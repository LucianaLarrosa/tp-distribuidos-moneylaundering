import os

from common.worker.worker_config import WorkerConfig


class StatefulWorkerConfig(WorkerConfig):
    def __init__(self):
        super().__init__()
        self.control_exchange = os.environ["CONTROL_EXCHANGE"]
        self.node_id = int(os.environ["NODE_ID"])
        self.node_prefix = os.environ["NODE_PREFIX"]
        self.ring_size = int(os.environ["RING_SIZE"])
