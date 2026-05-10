import signal

from client.config import Config


class Client:
    def __init__(self, config: Config) -> None:
        pass

    def run(self) -> None:
        pass

    def shutdown(self) -> None:
        pass


def main() -> None:
    config = Config.from_env()
    client = Client(config)
    signal.signal(signal.SIGTERM, client.shutdown)
    client.run()


if __name__ == "__main__":
    main()
