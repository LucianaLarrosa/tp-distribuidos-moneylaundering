import os


class Config:
    def __init__(self):
        self.rabbitmq_host = os.environ["RABBITMQ_HOST"]
        self.input_exchange = os.environ["INPUT_EXCHANGE"]
        self.input_queue = os.environ["INPUT_QUEUE"]
        self.output_exchange = os.environ["OUTPUT_EXCHANGE"]
        self.control_exchange = os.environ["CONTROL_EXCHANGE"]
        self.node_id = int(os.environ["NODE_ID"])
        self.node_prefix = os.environ["NODE_PREFIX"]
        self.ring_size = int(os.environ["RING_SIZE"])
        self.batch_size = int(os.environ["BATCH_SIZE"])
        self.num_shards = int(os.environ["NUM_SHARDS"])
