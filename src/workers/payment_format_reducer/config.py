import os

from common.worker.stateful_worker_config import StatefulWorkerConfig


class Config(StatefulWorkerConfig):
    def __init__(self):
        super().__init__()
        self.input_exchange = os.environ["INPUT_EXCHANGE"]
        self.shard_id = os.environ["SHARD_ID"]
        self.output_exchange = os.environ["OUTPUT_EXCHANGE"]
