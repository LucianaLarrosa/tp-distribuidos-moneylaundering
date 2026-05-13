import os
from dataclasses import dataclass


@dataclass
class Config:
    listen_host: str
    listen_port: int
    pool_size: int
    debug_output_dir: str

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            listen_host=os.environ.get("GATEWAY_HOST", ""),
            listen_port=int(os.environ.get("GATEWAY_PORT", 5000)),
            pool_size=int(os.environ.get("POOL_SIZE", os.cpu_count() or 4)),
            debug_output_dir=os.environ.get("DEBUG_OUTPUT_DIR", ""),
        )