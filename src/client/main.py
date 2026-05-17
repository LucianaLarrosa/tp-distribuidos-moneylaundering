import logging
import signal

from client.config import Config
from common.socket.safe_socket import SafeSocket
from common.models.raw_transaction import RawTransaction
from common.models.raw_account import RawAccount
from common.protocol import external
from common.protocol.external import MsgType


class Client:
    def __init__(self, config):
        self._config = config
        self._sock = None
        self._closed = False

    def run(self):
        logging.info(
            f"Connecting to {self._config.server_host}:{self._config.server_port}"
        )
        self._sock = SafeSocket.connect(
            self._config.server_host, self._config.server_port
        )
        logging.info("Connected")

        try:
            self._send_accounts()
            self._send_transactions()
            self._send_eof_and_wait_ack()
        finally:
            self._disconnect()

    def _send_accounts(self):
        total = self._send_batches(
            self._config.input_csv_accounts, MsgType.ACCOUNT_BATCH, RawAccount
        )
        logging.info(f"Sent {total} accounts")

    def _send_transactions(self):
        total = self._send_batches(
            self._config.input_csv_transactions,
            MsgType.TRANSACTION_BATCH,
            RawTransaction,
        )
        logging.info(f"Sent {total} transactions")

    def _send_batches(self, csv_path, msg_type, data_class_type):
        batch = []
        total = 0

        with open(csv_path) as f:
            next(f)

            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                batch.append(data_class_type(raw=line))
                if len(batch) >= self._config.batch_size:
                    external.send_msg(self._sock, msg_type, batch)
                    total += len(batch)
                    batch = []
        if batch:
            external.send_msg(self._sock, msg_type, batch)
            total += len(batch)
        return total

    def _send_eof_and_wait_ack(self):
        logging.info("Sending EOF")
        external.send_msg(self._sock, MsgType.EOF)

        msg_type, _ = external.recv_msg(self._sock)
        if msg_type != MsgType.ACK:
            raise RuntimeError(f"Expected ACK, got msg_type={msg_type}")
        logging.info("ACK received")

    def _disconnect(self):
        if self._sock is not None:
            self._sock.close()
            self._sock = None
        self._closed = True

    def shutdown(self, signum=None, frame=None):
        logging.info("Shutdown requested")
        if self._closed:
            return
        self._disconnect()


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    config = Config.from_env()
    client = Client(config)
    signal.signal(signal.SIGTERM, client.shutdown)
    client.run()


if __name__ == "__main__":
    main()
