import os

from common.worker.side_input_worker_config import SideInputWorkerConfig


class Config(SideInputWorkerConfig):
    def __init__(self):
        super().__init__()
        self.input_exchange = os.environ["INPUT_EXCHANGE"]
        self.output_exchange = os.environ["OUTPUT_EXCHANGE"]
        self.query_id = int(os.environ["QUERY_ID"])
        self.banks_exchange = os.environ["BANKS_EXCHANGE"]
        self.spill_dir = os.environ.get("SPILL_DIR", "/tmp/bank_mapper")
        self.side_input_node_prefix = os.environ.get(
            "SIDE_INPUT_NODE_PREFIX", "bank_mapper_side_input_node_"
        )
        self.shard_id = os.environ["SHARD_ID"]
