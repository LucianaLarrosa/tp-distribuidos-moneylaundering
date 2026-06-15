import os


class WorkerConfig:
    def __init__(self):
        self.node_name = os.environ["NODE_NAME"]
        self.ping_port = int(os.environ["PING_PORT"])
        self.ping_pong_host = os.environ["PING_PONG_HOST"]
