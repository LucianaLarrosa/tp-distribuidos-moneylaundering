import os

from common.worker.worker_config import WorkerConfig


class Config(WorkerConfig):
    def __init__(self):
        super().__init__()
        self.rabbitmq_host = os.environ.get("RABBITMQ_HOST", "rabbitmq")
        self.raw_data_exchange = os.environ.get("RAW_DATA_EXCHANGE", "raw_data")
        self.input_routing_key = os.environ.get("INPUT_ROUTING_KEY", "transaction")
        self.input_queue_name = os.environ.get("INPUT_QUEUE_NAME")
        self.output_exchange = os.environ.get(
            "OUTPUT_EXCHANGE", "filtered_transactions"
        )
        self.output_routing_key_usd = os.environ.get("OUTPUT_ROUTING_KEY_USD", "usd")
        self.output_routing_key_all = os.environ.get("OUTPUT_ROUTING_KEY_ALL", "all")
        self.output_routing_key_eof = os.environ.get("OUTPUT_ROUTING_KEY_EOF", "eof")
        self.usd_currency = os.environ.get("USD_CURRENCY", "us dollar")
        self.bank_max_exchange = os.environ.get("BANK_MAX_EXCHANGE", "bank_max_input")
        self.bank_max_node_count = int(os.environ.get("BANK_MAX_NODE_COUNT", "1"))
