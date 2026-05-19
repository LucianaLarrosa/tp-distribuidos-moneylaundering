from dataclasses import dataclass


@dataclass
class PaymentFormatPartial:
    payment_format: str
    total_amount: float
    count: int
