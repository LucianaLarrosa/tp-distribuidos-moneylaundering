import os
from dataclasses import dataclass


@dataclass
class Config:
    listen_host: str
    listen_port: int
    pool_size: int
    rabbitmq_host: str
    raw_data_exchange: str
    transaction_routing_key: str
    account_routing_key: str
    query_results_exchange: str
    results_queue_prefix: str
    node_name: str
    ping_port: int
    ping_pong_host: str

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            listen_host=os.environ.get("GATEWAY_HOST", ""),
            listen_port=int(os.environ.get("GATEWAY_PORT", 5000)),
            pool_size=int(os.environ.get("POOL_SIZE", os.cpu_count() or 4)),
            rabbitmq_host=os.environ.get("RABBITMQ_HOST", "rabbitmq"),
            raw_data_exchange=os.environ.get("RAW_DATA_EXCHANGE", "raw_data"),
            transaction_routing_key=os.environ.get(
                "TRANSACTION_ROUTING_KEY", "transaction"
            ),
            account_routing_key=os.environ.get("ACCOUNT_ROUTING_KEY", "account"),
            query_results_exchange=os.environ.get(
                "QUERY_RESULTS_EXCHANGE", "query_results"
            ),
            results_queue_prefix=os.environ.get("RESULTS_QUEUE_PREFIX", "results"),
            node_name=os.environ.get("NODE_NAME", "gateway"),
            ping_port=int(os.environ.get("PING_PORT", 9001)),
            ping_pong_host=os.environ.get("PING_PONG_HOST", "0.0.0.0"),
        )
