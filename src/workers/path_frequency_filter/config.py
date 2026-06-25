import os

from common.worker.stateful_worker_config import StatefulWorkerConfig


class Config(StatefulWorkerConfig):
    def __init__(self):
        super().__init__()
        self.input_exchange = os.environ["INPUT_EXCHANGE"]
        self.output_exchange = os.environ["OUTPUT_EXCHANGE"]
        self.output_node_count = int(os.environ["OUTPUT_NODE_COUNT"])
        self.output_node_prefix = os.environ["OUTPUT_NODE_PREFIX"]
        self.min_required_accounts = int(os.environ["MIN_REQUIRED_ACCOUNTS"])
        self.batch_size = int(os.environ["BATCH_SIZE"])
