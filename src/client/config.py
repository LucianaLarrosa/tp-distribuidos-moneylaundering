import os
from dataclasses import dataclass


@dataclass
class Config:
    server_host: str
    server_port: int
    input_csv_transactions: str
    input_csv_accounts: str
    batch_size: int

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            server_host=os.environ["SERVER_HOST"],
            server_port=int(os.environ["SERVER_PORT"]),
            input_csv_transactions=os.environ["INPUT_CSV_TRANSACTIONS"],
            input_csv_accounts=os.environ["INPUT_CSV_ACCOUNTS"],
            batch_size=int(os.environ.get("BATCH_SIZE", 1000)),
        )
