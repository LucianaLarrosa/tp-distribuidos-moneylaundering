import signal

from gateway.config import Config


class Gateway:
    def __init__(self, config: Config) -> None:
        pass

    def run(self) -> None:
        pass

    def shutdown(self) -> None:
        pass


def main() -> None:
    config = Config.from_env()
    gateway = Gateway(config)
    signal.signal(signal.SIGTERM, gateway.shutdown)
    gateway.run()


if __name__ == "__main__":
    main()
