import os
from datetime import datetime


class Config:
    def __init__(self):
        self.rabbitmq_host = os.environ["RABBITMQ_HOST"]
        self.input_exchange = os.environ["INPUT_EXCHANGE"]
        self.input_routing_keys = [
            routing_key.strip()
            for routing_key in os.environ["INPUT_ROUTING_KEY"].split(",")
        ]
        self.input_queue_name = os.environ.get("INPUT_QUEUE_NAME")
        self.output_exchange = os.environ["OUTPUT_EXCHANGE"]
        self.date_format = os.environ["DATE_FORMAT"]
        self.date_from_1 = datetime.strptime(
            os.environ["DATE_FROM_1"], self.date_format
        )
        self.date_to_1 = datetime.strptime(os.environ["DATE_TO_1"], self.date_format)
        self.date_from_2 = datetime.strptime(
            os.environ["DATE_FROM_2"], self.date_format
        )
        self.date_to_2 = datetime.strptime(os.environ["DATE_TO_2"], self.date_format)
        self.usd_currency = os.environ["USD_CURRENCY"].lower()
        self.output_routing_key_usd = os.environ["OUTPUT_ROUTING_KEY_USD"]
        self.output_routing_key_all = os.environ["OUTPUT_ROUTING_KEY_ALL"]
        self.output_routing_key_period_1 = os.environ["OUTPUT_ROUTING_KEY_PERIOD_1"]
        self.output_routing_key_period_2 = os.environ["OUTPUT_ROUTING_KEY_PERIOD_2"]
        self.output_routing_key_eof = os.environ["OUTPUT_ROUTING_KEY_EOF"]
