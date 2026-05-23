import os
from dataclasses import dataclass
from typing import List


@dataclass
class Config:
    proxy_host: str
    proxy_port: int
    input_csv_transactions: str
    input_csv_accounts: str
    batch_size: int
    expected_query_ids: List[int]
    output_dir: str
    client_id: str

    @classmethod
    def from_env(cls) -> "Config":
        raw_ids = os.environ.get("EXPECTED_QUERY_IDS", "5")
        expected_query_ids = [
            int(qid.strip()) for qid in raw_ids.split(",") if qid.strip()
        ]
        return cls(
            proxy_host=os.environ["PROXY_HOST"],
            proxy_port=int(os.environ["PROXY_PORT"]),
            input_csv_transactions=os.environ["INPUT_CSV_TRANSACTIONS"],
            input_csv_accounts=os.environ["INPUT_CSV_ACCOUNTS"],
            batch_size=int(os.environ.get("BATCH_SIZE", 1000)),
            expected_query_ids=expected_query_ids,
            output_dir=os.environ.get("OUTPUT_DIR", "results"),
            client_id=os.environ.get("CLIENT_ID", "1"),
        )
