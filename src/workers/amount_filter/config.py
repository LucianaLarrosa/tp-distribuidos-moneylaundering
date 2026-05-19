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
    control_exchange: str
    node_id: int
    node_prefix: str
    ring_size: int
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
            control_exchange=os.environ.get("CONTROL_EXCHANGE", "amount_filter_control"),
            node_id=int(os.environ["NODE_ID"]),
            node_prefix=os.environ.get("NODE_PREFIX", "amount_filter_node_"),
            ring_size=int(os.environ["RING_SIZE"]),
            amount_threshold=float(os.environ.get("AMOUNT_THRESHOLD", 50.0)),
        )
