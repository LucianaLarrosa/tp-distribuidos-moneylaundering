import os
from dataclasses import dataclass


@dataclass
class Config:
    rabbitmq_host: str
    input_exchange: str
    input_routing_key: str
    input_eof_routing_key: str
    input_queue_name: str
    output_exchange: str
    amount_threshold: float

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            rabbitmq_host=os.environ.get("RABBITMQ_HOST", "rabbitmq"),
            input_exchange=os.environ.get("INPUT_EXCHANGE", "filtered_transactions"),
            input_routing_key=os.environ.get("INPUT_ROUTING_KEY", "usd"),
            input_eof_routing_key=os.environ.get("INPUT_EOF_ROUTING_KEY", "eof"),
            input_queue_name=os.environ.get("INPUT_QUEUE_NAME", "amount_filter_input"),
            output_exchange=os.environ.get("OUTPUT_EXCHANGE", "query_results"),
            amount_threshold=float(os.environ.get("AMOUNT_THRESHOLD", 50.0)),
        )
