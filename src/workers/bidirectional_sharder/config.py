import os

from common.worker.worker_config import WorkerConfig


class Config(WorkerConfig):
    ENV_SEPARATOR = ","

    def __init__(self):
        super().__init__()
        self.rabbitmq_host = os.environ["RABBITMQ_HOST"]
        self.input_exchange = os.environ["INPUT_EXCHANGE"]
        self.input_routing_keys = [
            routing_key.strip()
            for routing_key in os.environ["INPUT_ROUTING_KEY"].split(self.ENV_SEPARATOR)
        ]
        self.input_queue_name = os.environ.get("INPUT_QUEUE_NAME")
        self.output_exchange = os.environ["OUTPUT_EXCHANGE"]
        self.control_exchange = os.environ["CONTROL_EXCHANGE"]
        self.node_id = int(os.environ["NODE_ID"])
        self.ring_size = int(os.environ["RING_SIZE"])
        self.node_prefix = os.environ["NODE_PREFIX"]
        self.output_node_count = int(os.environ["OUTPUT_NODE_COUNT"])
        self.output_node_prefix = os.environ["OUTPUT_NODE_PREFIX"]
