from abc import ABC, abstractmethod


class Worker(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def _handle_message(self, message, ack, nack):
        pass

    @abstractmethod
    def shutdown(self):
        pass
