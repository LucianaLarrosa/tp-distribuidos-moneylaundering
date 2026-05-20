from dataclasses import dataclass


@dataclass
class Q1Result:
    from_bank: str
    from_account: str
    to_bank: str
    to_account: str
    amount_paid: float


@dataclass
class Q2Result:
    bank_name: str
    from_account: str
    amount_paid: float


@dataclass
class Q3Result:
    from_bank: str
    from_account: str
    amount_paid: float


@dataclass
class Q4Result:
    from_bank: str
    from_account: str


@dataclass
class Q5Result:
    count: int
