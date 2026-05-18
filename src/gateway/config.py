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
        )
