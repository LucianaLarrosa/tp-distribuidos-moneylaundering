from dataclasses import dataclass


@dataclass
class Path:
    from_bank: str
    from_account: str
    mid_bank: str
    mid_account: str
    to_bank: str
    to_account: str
