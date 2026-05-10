from typing import Callable, List

from common.middleware.middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
)


class RabbitMQQueue(MessageMiddlewareQueue):
    def __init__(self, host: str, queue_name: str) -> None:
        pass

    def start_consuming(self, on_message_callback: Callable[[bytes], None]) -> None:
        pass

    def stop_consuming(self) -> None:
        pass

    def send(self, message: bytes) -> None:
        pass

    def close(self) -> None:
        pass


class RabbitMQDirectExchange(MessageMiddlewareExchange):
    def __init__(self, host: str, exchange_name: str, route_keys: List[str]) -> None:
        pass

    def start_consuming(self, on_message_callback: Callable[[bytes], None]) -> None:
        pass

    def stop_consuming(self) -> None:
        pass

    def send(self, message: bytes) -> None:
        pass

    def close(self) -> None:
        pass


class RabbitMQFanoutExchange(MessageMiddlewareExchange):
    def __init__(self, host: str, exchange_name: str) -> None:
        pass

    def start_consuming(self, on_message_callback: Callable[[bytes], None]) -> None:
        pass

    def stop_consuming(self) -> None:
        pass

    def send(self, message: bytes) -> None:
        pass

    def close(self) -> None:
        pass


class RabbitMQTopicExchange(MessageMiddlewareExchange):
    def __init__(self, host: str, exchange_name: str, route_keys: List[str]) -> None:
        pass

    def start_consuming(self, on_message_callback: Callable[[bytes], None]) -> None:
        pass

    def stop_consuming(self) -> None:
        pass

    def send(self, message: bytes) -> None:
        pass

    def close(self) -> None:
        pass
