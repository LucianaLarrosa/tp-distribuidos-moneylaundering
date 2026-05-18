import os
from dataclasses import dataclass


@dataclass
class Config:
    rabbitmq_host: str
    raw_data_exchange: str
    input_routing_key: str
    output_exchange: str

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            rabbitmq_host=os.environ.get("RABBITMQ_HOST", "rabbitmq"),
            raw_data_exchange=os.environ.get("RAW_DATA_EXCHANGE", "raw_data"),
            input_routing_key=os.environ.get("INPUT_ROUTING_KEY", "account"),
            output_exchange=os.environ.get("OUTPUT_EXCHANGE", "filtered_accounts"),
        )
