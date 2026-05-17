from dataclasses import dataclass


@dataclass
class BankMaxPartial:
    from_bank: int
    from_account: str
    amount: float
