import os
from dataclasses import dataclass


@dataclass
class Config:
    rabbitmq_host: str
    raw_data_exchange: str
    input_routing_key: str
    input_queue_name: str
    output_exchange: str
    output_routing_keys: list[str]
    output_node_count: int
    output_node_prefix: str

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            rabbitmq_host=os.environ.get("RABBITMQ_HOST", "rabbitmq"),
            raw_data_exchange=os.environ.get("RAW_DATA_EXCHANGE", "raw_data"),
            input_routing_key=os.environ.get("INPUT_ROUTING_KEY", "account"),
            input_queue_name=os.environ.get("INPUT_QUEUE_NAME"),
            output_exchange=os.environ.get("OUTPUT_EXCHANGE", "filtered_accounts"),
            output_routing_keys=os.environ.get("OUTPUT_ROUTING_KEYS", "1").strip().split(","),
            output_node_count=int(os.environ.get("OUTPUT_NODE_COUNT", "1")),
            output_node_prefix=os.environ.get("OUTPUT_NODE_PREFIX", "bank_mapper_side_input_node_"),
        )
