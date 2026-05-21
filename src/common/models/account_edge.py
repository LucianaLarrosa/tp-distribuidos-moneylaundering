from dataclasses import dataclass


@dataclass
class AccountEdge:
    bank: str
    account: str
    other_bank: str
    other_account: str
    is_sender: bool
