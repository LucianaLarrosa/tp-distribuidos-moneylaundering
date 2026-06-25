import os

from common.worker.stateful_worker_config import StatefulWorkerConfig


class Config(StatefulWorkerConfig):
    def __init__(self):
        super().__init__()
        self.input_exchange = os.environ["INPUT_EXCHANGE"]
        self.input_queue = os.environ["INPUT_QUEUE"]
        self.output_exchange = os.environ["OUTPUT_EXCHANGE"]
        self.batch_size = int(os.environ["BATCH_SIZE"])
        self.num_shards = int(os.environ["NUM_SHARDS"])
