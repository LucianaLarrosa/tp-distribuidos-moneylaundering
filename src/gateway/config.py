import os
from dataclasses import dataclass


@dataclass
class Config:
    listen_host: str
    listen_port: int
    debug_output_file: str

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            listen_host=os.environ.get("GATEWAY_HOST", ""),
            listen_port=int(os.environ.get("GATEWAY_PORT", 5000)),
            debug_output_file=os.environ.get("DEBUG_OUTPUT_FILE", ""),
        )