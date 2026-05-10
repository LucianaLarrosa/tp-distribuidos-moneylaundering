from abc import ABC, abstractmethod
from typing import Callable


class MessageMiddleware(ABC):
    @abstractmethod
    def start_consuming(self, on_message_callback: Callable[[bytes], None]) -> None:
        pass

    @abstractmethod
    def stop_consuming(self) -> None:
        pass

    @abstractmethod
    def send(self, message: bytes) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class MessageMiddlewareExchange(MessageMiddleware):
    pass


class MessageMiddlewareQueue(MessageMiddleware):
    pass
