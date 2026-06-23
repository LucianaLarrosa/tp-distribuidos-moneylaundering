from dataclasses import dataclass

CLEANUP_EXPECTED_COUNT = -1


def is_cleanup_eof(eof):
    return eof.message_count == CLEANUP_EXPECTED_COUNT


@dataclass
class EOF:
    message_count: int


@dataclass
class RingEOF:
    expected_count: int
    total_processed_count: int
    coordinator_id: int | None = None
    total_sent_count: int | None = None  # Only used by sharders
