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
        self.output_queue = os.environ["OUTPUT_QUEUE"]
        self.valid_payment_formats = {
            payment_format.strip().lower()
            for payment_format in os.environ["VALID_PAYMENT_FORMATS"].split(
                self.ENV_SEPARATOR
            )
        }
