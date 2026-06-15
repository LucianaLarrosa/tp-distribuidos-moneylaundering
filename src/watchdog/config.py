import os


class Config:
    ENV_SEPARATOR = ","

    def __init__(self):
        self.ping_port = int(os.environ["PING_PORT"])
        self.pong_port = int(os.environ["PONG_PORT"])
        self.ping_timeout_seconds = float(os.environ["PING_TIMEOUT_SECONDS"])
        self.check_interval_seconds = float(os.environ["CHECK_INTERVAL_SECONDS"])
        self.max_retries = int(os.environ["MAX_RETRIES"])
        self.ping_pong_host = os.environ["PING_PONG_HOST"]
        self.monitored_nodes = [
            node
            for node in os.environ["MONITORED_NODES"].split(self.ENV_SEPARATOR)
            if node.strip()
        ]
        self.watchdog_id = int(os.environ["WATCHDOG_ID"])
        self.peers = {
            i: f"watchdog_{i}" for i in range(int(os.environ["WATCHDOG_COUNT"]))
        }
        self.node_name = self.peers[self.watchdog_id]
        self.election_port = int(os.environ["ELECTION_PORT"])
        self.election_timeout_seconds = float(os.environ["ELECTION_TIMEOUT_SECONDS"])
        self.leader_probe_miss_threshold = int(
            os.environ["LEADER_PROBE_MISS_THRESHOLD"]
        )
