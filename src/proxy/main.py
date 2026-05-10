import signal

from proxy.config import Config


class Proxy:
    def __init__(self, config: Config) -> None:
        pass

    def run(self) -> None:
        pass

    def shutdown(self) -> None:
        pass


def main() -> None:
    config = Config.from_env()
    proxy = Proxy(config)
    signal.signal(signal.SIGTERM, proxy.shutdown)
    proxy.run()


if __name__ == "__main__":
    main()
