from dataclasses import dataclass


@dataclass
class PaymentFormatAverage:
    payment_format: str
    average_amount: float
