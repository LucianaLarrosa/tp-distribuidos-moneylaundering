import os


class Config:
    ENV_SEPARATOR = ","

    def __init__(self):
        raw_nodes = os.environ.get("MONITORED_NODES")
        self.monitored_nodes = [
            node for node in raw_nodes.split(self.ENV_SEPARATOR) if node.strip()
        ]
        self.port = int(os.environ.get("WATCHDOG_PORT"))
        self.timeout_seconds = float(os.environ.get("TIMEOUT_SECONDS"))
        self.check_interval_seconds = float(os.environ.get("CHECK_INTERVAL_SECONDS"))
