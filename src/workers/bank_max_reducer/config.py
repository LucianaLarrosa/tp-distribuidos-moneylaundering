import os

from common.worker.stateful_worker_config import StatefulWorkerConfig


class Config(StatefulWorkerConfig):
    def __init__(self):
        super().__init__()
        self.input_exchange = os.environ["INPUT_EXCHANGE"]
        self.shard_id = os.environ["SHARD_ID"]
        self.output_exchange = os.environ["OUTPUT_EXCHANGE"]
        self.batch_size = int(os.environ["BATCH_SIZE"])
        self.output_routing_keys = os.environ["OUTPUT_ROUTING_KEYS"].strip().split(",")
        self.output_node_count = int(os.environ["OUTPUT_NODE_COUNT"])
