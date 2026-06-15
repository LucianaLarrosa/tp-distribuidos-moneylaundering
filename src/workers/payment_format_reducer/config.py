import os

from common.worker.worker_config import WorkerConfig


class Config(WorkerConfig):
    def __init__(self):
        super().__init__()
        self.rabbitmq_host = os.environ["RABBITMQ_HOST"]
        self.input_exchange = os.environ["INPUT_EXCHANGE"]
        self.shard_id = os.environ["SHARD_ID"]
        self.output_exchange = os.environ["OUTPUT_EXCHANGE"]
        self.control_exchange = os.environ["CONTROL_EXCHANGE"]
        self.node_id = int(os.environ["NODE_ID"])
        self.node_prefix = os.environ["NODE_PREFIX"]
        self.ring_size = int(os.environ["RING_SIZE"])
