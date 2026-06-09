import os


class WorkerConfig:
    def __init__(self):
        self.node_name = os.environ["NODE_NAME"]
        self.watchdog_host = os.environ["WATCHDOG_HOST"]
        self.watchdog_port = int(os.environ["WATCHDOG_PORT"])
        self.heartbeat_interval_seconds = float(os.environ["HEARTBEAT_INTERVAL_SECONDS"])
