import os

from common.worker.worker_config import WorkerConfig


class Config(WorkerConfig):
    def __init__(self):
        super().__init__()
        self.raw_data_exchange = os.environ.get("RAW_DATA_EXCHANGE", "raw_data")
        self.input_routing_key = os.environ.get("INPUT_ROUTING_KEY", "account")
        self.input_queue_name = os.environ.get("INPUT_QUEUE_NAME")
        self.output_exchange = os.environ.get("OUTPUT_EXCHANGE", "filtered_accounts")
        self.output_routing_keys = (
            os.environ.get("OUTPUT_ROUTING_KEYS", "1").strip().split(",")
        )
        self.output_node_count = int(os.environ.get("OUTPUT_NODE_COUNT", "1"))
        self.output_node_prefix = os.environ.get(
            "OUTPUT_NODE_PREFIX", "bank_mapper_side_input_node_"
        )
