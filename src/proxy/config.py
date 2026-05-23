import os
from dataclasses import dataclass
from typing import List


@dataclass
class Config:
    listen_host: str
    listen_port: int
    gateway_hosts: List[str]
    gateway_port: int

    @classmethod
    def from_env(cls) -> "Config":
        raw_hosts = os.environ["GATEWAY_HOSTS"]
        gateway_hosts = [h.strip() for h in raw_hosts.split(",") if h.strip()]
        return cls(
            listen_host=os.environ.get("PROXY_HOST", ""),
            listen_port=int(os.environ.get("PROXY_PORT", 6000)),
            gateway_hosts=gateway_hosts,
            gateway_port=int(os.environ["GATEWAY_PORT"]),
        )
