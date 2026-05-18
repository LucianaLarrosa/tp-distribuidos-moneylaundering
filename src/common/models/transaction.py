from dataclasses import dataclass
from datetime import datetime


@dataclass
class Transaction:
    timestamp: datetime
    from_bank: str
    from_account: str
    to_bank: str
    to_account: str
    amount: float
    currency: str
    payment_format: str
