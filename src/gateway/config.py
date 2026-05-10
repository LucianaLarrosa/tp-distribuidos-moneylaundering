from dataclasses import dataclass


@dataclass
class Config:
    listen_port: int
    rabbitmq_host: str
    ingress_exchange: str
    results_queue: str

    @classmethod
    def from_env(cls) -> "Config":
        pass
