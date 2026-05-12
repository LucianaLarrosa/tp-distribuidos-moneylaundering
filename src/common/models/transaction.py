from dataclasses import dataclass
from datetime import datetime


@dataclass
class Transaction:
    timestamp: datetime
    from_bank: int
    from_account: str
    to_bank: int
    to_account: str
    amount_received: float
    receiving_currency: str
    amount_paid: float
    payment_currency: str
    payment_format: str
    is_laundering: bool
