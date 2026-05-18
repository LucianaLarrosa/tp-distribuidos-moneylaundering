import os
from dataclasses import dataclass


@dataclass
class Config:
    rabbitmq_host: str
    raw_data_exchange: str
    input_routing_key: str
    output_exchange: str
    output_routing_key_usd: str
    output_routing_key_nousd: str
    output_routing_key_eof: str
    usd_currency: str

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            rabbitmq_host=os.environ.get("RABBITMQ_HOST", "rabbitmq"),
            raw_data_exchange=os.environ.get("RAW_DATA_EXCHANGE", "raw_data"),
            input_routing_key=os.environ.get("INPUT_ROUTING_KEY", "transaction"),
            output_exchange=os.environ.get("OUTPUT_EXCHANGE", "filtered_transactions"),
            output_routing_key_usd=os.environ.get("OUTPUT_ROUTING_KEY_USD", "usd"),
            output_routing_key_nousd=os.environ.get(
                "OUTPUT_ROUTING_KEY_NOUSD", "nousd"
            ),
            output_routing_key_eof=os.environ.get("OUTPUT_ROUTING_KEY_EOF", "eof"),
            usd_currency=os.environ.get("USD_CURRENCY", "us dollar"),
        )
