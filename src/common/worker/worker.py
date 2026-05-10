from abc import ABC, abstractmethod


class Worker(ABC):
    def __init__(self) -> None:
        pass

    def run(self) -> None:
        pass

    @abstractmethod
    def handle_message(self, raw: bytes) -> None:
        pass

    def shutdown(self) -> None:
        pass
