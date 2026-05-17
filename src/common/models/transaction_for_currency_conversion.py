from dataclasses import dataclass


@dataclass
class TransactionForCurrencyConversion:
    timestamp: str
    amount: float
    currency: str
