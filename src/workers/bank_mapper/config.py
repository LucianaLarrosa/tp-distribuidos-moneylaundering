import os


class Config:
    def __init__(self):
        self.rabbitmq_host = os.environ["RABBITMQ_HOST"]
        self.input_exchange = os.environ["INPUT_EXCHANGE"]
        self.output_exchange = os.environ["OUTPUT_EXCHANGE"]
        self.banks_exchange = os.environ["BANKS_EXCHANGE"]
        self.control_exchange = os.environ["CONTROL_EXCHANGE"]
        self.node_id = int(os.environ["NODE_ID"])
        self.node_prefix = os.environ["NODE_PREFIX"]
        self.ring_size = int(os.environ["RING_SIZE"])
        self.spill_dir = os.environ.get("SPILL_DIR", "/tmp/bank_mapper")
        self.side_input_node_prefix = os.environ.get("SIDE_INPUT_NODE_PREFIX", "bank_mapper_side_input_node_")
        self.shard_id = os.environ["SHARD_ID"]
