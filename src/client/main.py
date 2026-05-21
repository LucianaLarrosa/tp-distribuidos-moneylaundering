import csv
import logging
import os
import signal
import threading
from dataclasses import asdict

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
            "Connecting to %s:%s",
            self._config.server_host,
            self._config.server_port,
        )
        self._sock = SafeSocket.connect(
            self._config.server_host, self._config.server_port
        )

        self._receiver_thread = threading.Thread(
            target=self._wait_for_query_results, daemon=True
        )
        self._receiver_thread.start()

        try:
            self._send_accounts()
            self._send_eof(MsgType.EOF_ACCOUNTS)
            self._send_transactions()
            self._send_eof(MsgType.EOF_TRANSACTIONS)
            self._receiver_thread.join()
        finally:
            self._disconnect()

    def _send_accounts(self):
        total = self._send_batches(
            self._config.input_csv_accounts, MsgType.ACCOUNT_BATCH, RawAccount
        )
        logging.info("Sent %s accounts", total)

    def _send_transactions(self):
        total = self._send_batches(
            self._config.input_csv_transactions,
            MsgType.TRANSACTION_BATCH,
            RawTransaction,
        )
        logging.info("Sent %s transactions", total)

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

    def _send_eof(self, eof_msg_type):
        external.send_msg(self._sock, eof_msg_type)

    def _wait_for_query_results(self):
        pending = set(self._config.expected_query_ids)
        if not pending:
            return
        os.makedirs(self._config.output_dir, exist_ok=True)
        files = {
            qid: open(
                os.path.join(self._config.output_dir, f"q{qid}.csv"), "w", newline=""
            )
            for qid in pending
        }
        writers = {qid: csv.writer(f) for qid, f in files.items()}
        totals = {qid: 0 for qid in pending}
        try:
            while pending:
                msg_type, payload = external.recv_msg(self._sock)
                if msg_type == MsgType.QUERY_RESULT:
                    query_id, records = payload
                    for record in records:
                        writers[query_id].writerow(asdict(record).values())
                    totals[query_id] += len(records)
                    logging.info(
                        "Q%s: received %s record(s) (total so far: %s)",
                        query_id,
                        len(records),
                        totals[query_id],
                    )
                elif msg_type == MsgType.QUERY_END:
                    query_id = payload
                    pending.discard(query_id)
                    logging.info(
                        "Q%s ended (%s records total)",
                        query_id,
                        totals[query_id],
                    )
                else:
                    logging.warning(
                        "Unexpected msg_type=%s while waiting for results",
                        msg_type,
                    )
        finally:
            for f in files.values():
                f.close()
        logging.info("All expected query results received")

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
