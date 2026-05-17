import os

ENV_SEPARATOR = ","


class Config:
    def __init__(self):
        self.rabbitmq_host = os.environ["RABBITMQ_HOST"]
        self.input_exchange = os.environ["INPUT_EXCHANGE"]
        self.input_routing_keys = [
            routing_key.strip()
            for routing_key in os.environ["INPUT_ROUTING_KEY"].split(ENV_SEPARATOR)
        ]
        self.output_queue = os.environ["OUTPUT_QUEUE"]
        self.valid_payment_formats = {
            payment_format.strip().lower()
            for payment_format in os.environ["VALID_PAYMENT_FORMATS"].split(
                ENV_SEPARATOR
            )
        }
