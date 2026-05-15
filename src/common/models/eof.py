from dataclasses import dataclass


@dataclass
class EOF:
    message_count: int


@dataclass
class RingEOF:
    expected_count: int
    total_processed_count: int
    coordinator_id: int | None = None
    total_sent_count: int | None = None  # Only used by sharders
