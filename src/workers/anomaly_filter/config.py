import os

from common.worker.side_input_worker_config import SideInputWorkerConfig


class Config(SideInputWorkerConfig):
    def __init__(self):
        super().__init__()
        self.input_exchange = os.environ["INPUT_EXCHANGE"]
        self.input_queue = os.environ["INPUT_QUEUE"]
        self.avg_exchange = os.environ["AVG_EXCHANGE"]
        self.output_exchange = os.environ["OUTPUT_EXCHANGE"]
        self.query_id = int(os.environ["QUERY_ID"])
        self.spill_dir = os.environ.get("SPILL_DIR", "/tmp/anomaly_filter")
        self.anomaly_threshold = float(os.environ.get("ANOMALY_THRESHOLD", "0.01"))
