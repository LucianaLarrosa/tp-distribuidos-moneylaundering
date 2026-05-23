import os


class Config:
    def __init__(self):
        self.rabbitmq_host = os.environ["RABBITMQ_HOST"]
        self.input_queue = os.environ["INPUT_QUEUE"]
        self.output_queue = os.environ["OUTPUT_QUEUE"]
        self.control_exchange = os.environ["CONTROL_EXCHANGE"]
        self.node_id = int(os.environ.get("NODE_ID"))
        self.ring_size = int(os.environ.get("RING_SIZE"))
        self.node_prefix = os.environ.get("NODE_PREFIX")
        self.amount_threshold = float(os.environ.get("AMOUNT_THRESHOLD"))
