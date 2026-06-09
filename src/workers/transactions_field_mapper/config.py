import os
from dataclasses import dataclass


@dataclass
class Config:
    rabbitmq_host: str
    raw_data_exchange: str
    input_routing_key: str
    input_queue_name: str
    output_exchange: str
    output_routing_key_usd: str
    output_routing_key_all: str
    output_routing_key_eof: str
    usd_currency: str
    bank_max_exchange: str
    bank_max_node_count: int

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            rabbitmq_host=os.environ.get("RABBITMQ_HOST", "rabbitmq"),
            raw_data_exchange=os.environ.get("RAW_DATA_EXCHANGE", "raw_data"),
            input_routing_key=os.environ.get("INPUT_ROUTING_KEY", "transaction"),
            input_queue_name=os.environ.get("INPUT_QUEUE_NAME"),
            output_exchange=os.environ.get("OUTPUT_EXCHANGE", "filtered_transactions"),
            output_routing_key_usd=os.environ.get("OUTPUT_ROUTING_KEY_USD", "usd"),
            output_routing_key_all=os.environ.get("OUTPUT_ROUTING_KEY_ALL", "all"),
            output_routing_key_eof=os.environ.get("OUTPUT_ROUTING_KEY_EOF", "eof"),
            usd_currency=os.environ.get("USD_CURRENCY", "us dollar"),
            bank_max_exchange=os.environ.get("BANK_MAX_EXCHANGE", "bank_max_input"),
            bank_max_node_count=int(os.environ.get("BANK_MAX_NODE_COUNT", "1")),
        )
