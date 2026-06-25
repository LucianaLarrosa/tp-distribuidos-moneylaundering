import os

from common.worker.stateful_worker_config import StatefulWorkerConfig


class Config(StatefulWorkerConfig):
    def __init__(self):
        super().__init__()
        self.input_exchange = os.environ["INPUT_EXCHANGE"]
        self.output_exchange = os.environ["OUTPUT_EXCHANGE"]
        self.query_id = int(os.environ["QUERY_ID"])
        self.batch_size = int(os.environ["BATCH_SIZE"])
