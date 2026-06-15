import os

from common.worker.worker_config import WorkerConfig


class Config(WorkerConfig):
    def __init__(self):
        super().__init__()
        self.rabbitmq_host = os.environ.get("RABBITMQ_HOST", "rabbitmq")
        self.input_exchange = os.environ.get("INPUT_EXCHANGE", "filtered_transactions")
        self.input_routing_key = os.environ.get("INPUT_ROUTING_KEY", "usd")
        self.input_eof_routing_key = os.environ.get("INPUT_EOF_ROUTING_KEY", "eof")
        self.input_queue_name = os.environ.get(
            "INPUT_QUEUE_NAME", "amount_filter_input"
        )
        self.output_exchange = os.environ.get("OUTPUT_EXCHANGE", "query_results")
        self.query_id = int(os.environ["QUERY_ID"])
        self.amount_threshold = float(os.environ.get("AMOUNT_THRESHOLD", 50.0))
