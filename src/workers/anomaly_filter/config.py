import os

from common.worker.worker_config import WorkerConfig


class Config(WorkerConfig):
    def __init__(self):
        super().__init__()
        self.rabbitmq_host = os.environ["RABBITMQ_HOST"]
        self.input_exchange = os.environ["INPUT_EXCHANGE"]
        self.input_queue = os.environ["INPUT_QUEUE"]
        self.avg_exchange = os.environ["AVG_EXCHANGE"]
        self.output_exchange = os.environ["OUTPUT_EXCHANGE"]
        self.query_id = int(os.environ["QUERY_ID"])
        self.control_exchange = os.environ["CONTROL_EXCHANGE"]
        self.node_id = int(os.environ["NODE_ID"])
        self.node_prefix = os.environ["NODE_PREFIX"]
        self.ring_size = int(os.environ["RING_SIZE"])
        self.spill_dir = os.environ.get("SPILL_DIR", "/tmp/anomaly_filter")
        self.anomaly_threshold = float(os.environ.get("ANOMALY_THRESHOLD", "0.01"))
