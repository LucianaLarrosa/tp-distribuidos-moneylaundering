import os

from common.worker.worker_config import WorkerConfig


class Config(WorkerConfig):
    def __init__(self):
        super().__init__()
        self.rabbitmq_host = os.environ["RABBITMQ_HOST"]
        self.input_exchange = os.environ["INPUT_EXCHANGE"]
        self.input_queue = os.environ["INPUT_QUEUE"]
        self.output_queue = os.environ["OUTPUT_QUEUE"]
        self.control_exchange = os.environ["CONTROL_EXCHANGE"]
        self.node_id = int(os.environ.get("NODE_ID"))
        self.ring_size = int(os.environ.get("RING_SIZE"))
        self.node_prefix = os.environ.get("NODE_PREFIX")
        self.amount_threshold = float(os.environ.get("AMOUNT_THRESHOLD"))
