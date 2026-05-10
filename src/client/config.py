from dataclasses import dataclass


@dataclass
class Config:
    proxy_host: str
    proxy_port: int
    input_csv: str
    output_dir: str
    batch_size: int

    @classmethod
    def from_env(cls) -> "Config":
        pass
