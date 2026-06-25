import os

from common.worker.stateful_worker_config import StatefulWorkerConfig


class Config(StatefulWorkerConfig):
    def __init__(self):
        super().__init__()
        self.input_queue = os.environ["INPUT_QUEUE"]
        self.output_exchange = os.environ["OUTPUT_EXCHANGE"]
        self.query_id = int(os.environ["QUERY_ID"])
