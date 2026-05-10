from dataclasses import dataclass
from typing import List


@dataclass
class Config:
    listen_port: int
    gateway_hosts: List[str]
    gateway_port: int

    @classmethod
    def from_env(cls) -> "Config":
        pass
