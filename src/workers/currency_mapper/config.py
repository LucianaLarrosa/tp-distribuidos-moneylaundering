import os

from common.worker.worker_config import WorkerConfig


class Config(WorkerConfig):
    def __init__(self):
        super().__init__()
        self.rabbitmq_host = os.environ["RABBITMQ_HOST"]
        self.input_queue = os.environ["INPUT_QUEUE"]
        self.output_queue = os.environ["OUTPUT_QUEUE"]
        self.target_currency = os.environ.get("TARGET_CURRENCY")
        self.frankfurter_url = os.environ["FRANKFURTER_URL"]
        self.frankfurter_timeout_seconds = int(
            os.environ.get("FRANKFURTER_TIMEOUT_SECONDS")
        )
        self.rates_date_field = os.environ.get("RATES_DATE_FIELD")
        self.rates_quote_field = os.environ.get("RATES_QUOTE_FIELD")
        self.rates_rate_field = os.environ.get("RATES_RATE_FIELD")
        self.rates_date_format = os.environ.get("RATES_DATE_FORMAT")
        self.transaction_date_format = os.environ.get("TRANSACTION_DATE_FORMAT")
