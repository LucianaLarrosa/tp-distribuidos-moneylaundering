import csv
import logging
import os
import signal
import threading
import queue
from dataclasses import asdict

from client.config import Config
from common.socket.safe_socket import SafeSocket, IncompleteReadError
from common.models.raw_transaction import RawTransaction
from common.models.raw_account import RawAccount
from common.protocol import external
from common.protocol.external import MsgType


class Client:
    def __init__(self, config):
        self._config = config
        self._sock = None
        self._proxy_sock = None
        self._receiver_thread = None
        self._receiver_queue = queue.Queue()
        self._sender_thread = None
        self._sender_queue = queue.Queue()
        self._aborted = threading.Event()
        self._closed = False

    def run(self):
        gateway_host, gateway_port = self._resolve_gateway()
        logging.info("Connecting to gateway %s:%s", gateway_host, gateway_port)
        self._sock = SafeSocket.connect(gateway_host, gateway_port)

        self._receiver_thread = threading.Thread(target=self._receive_data, daemon=True)
        self._sender_thread = threading.Thread(target=self._send_data, daemon=True)
        self._receiver_thread.start()
        self._sender_thread.start()

        try:
            self._orchestrate()
        finally:
            self._disconnect()
            if self._receiver_thread is not None:
                self._receiver_thread.join()
            if self._sender_thread is not None:
                self._sender_thread.join()

    def _orchestrate(self):
        pending = set(self._config.expected_query_ids)
        writers, totals, files = self._open_result_files(pending)
        try:
            self._produce_and_wait_acks(pending, writers, totals)
            self._consume_remaining(pending, writers, totals)
        finally:
            for f in files.values():
                f.close()
            self._sender_queue.put(None)
        if not pending:
            logging.info("All expected query results received")

    def _produce_and_wait_acks(self, pending, writers, totals):
        for msg_type, payload in self._messages_to_send():
            if self._aborted.is_set() or self._closed:
                return
            if payload is None:
                self._sender_queue.put((msg_type,))
            else:
                self._sender_queue.put((msg_type, payload))
            self._wait_ack(pending, writers, totals)

    def _messages_to_send(self):
        for batch in self._read_batches(
            self._config.input_csv_transactions, RawTransaction
        ):
            yield MsgType.TRANSACTION_BATCH, batch
        yield MsgType.EOF_TRANSACTIONS, None
        for batch in self._read_batches(self._config.input_csv_accounts, RawAccount):
            yield MsgType.ACCOUNT_BATCH, batch
        yield MsgType.EOF_ACCOUNTS, None

    def _wait_ack(self, pending, writers, totals):
        while not self._closed:
            item = self._receiver_queue.get()
            if item is None:
                return
            msg_type, payload = item
            if msg_type == MsgType.ACK:
                return
            elif msg_type == MsgType.QUERY_RESULT:
                self._handle_query_result(payload, writers, totals)
            elif msg_type == MsgType.QUERY_END:
                self._handle_query_end(payload, pending, totals)
            else:
                logging.warning("Unexpected msg_type=%s in orchestrator", msg_type)

    def _consume_remaining(self, pending, writers, totals):
        while pending and not self._closed:
            item = self._receiver_queue.get()
            if item is None:
                return
            msg_type, payload = item
            if msg_type == MsgType.QUERY_RESULT:
                self._handle_query_result(payload, writers, totals)
            elif msg_type == MsgType.QUERY_END:
                self._handle_query_end(payload, pending, totals)
            elif msg_type == MsgType.ACK:
                logging.warning("Unexpected ACK after all sends completed")
            else:
                logging.warning("Unexpected msg_type=%s in orchestrator", msg_type)

    def _open_result_files(self, query_ids):
        if not query_ids:
            return {}, {}, {}
        os.makedirs(self._config.output_dir, exist_ok=True)
        files = {
            qid: open(
                os.path.join(
                    self._config.output_dir,
                    f"q{qid}_client_{self._config.client_id}.csv",
                ),
                "w",
                newline="",
            )
            for qid in query_ids
        }
        writers = {qid: csv.writer(f) for qid, f in files.items()}
        totals = {qid: 0 for qid in query_ids}
        return writers, totals, files

    def _handle_query_result(self, payload, writers, totals):
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

    def _handle_query_end(self, payload, pending, totals):
        query_id = payload
        pending.discard(query_id)
        logging.info("Q%s ended (%s records total)", query_id, totals[query_id])

    def _resolve_gateway(self):
        logging.info("Connecting to proxy")
        self._proxy_sock = SafeSocket.connect(
            self._config.proxy_host, self._config.proxy_port
        )

        try:
            msg_type, payload = external.recv_msg(self._proxy_sock)
            if msg_type != MsgType.REDIRECT:
                raise RuntimeError(
                    f"Expected REDIRECT from proxy, got msg_type={msg_type}"
                )
            host, port = payload
            return host, port
        finally:
            if self._proxy_sock is not None:
                self._proxy_sock.close()
                self._proxy_sock = None

    def _receive_data(self):
        while True:
            try:
                msg_type, payload = external.recv_msg(self._sock)
            except IncompleteReadError as e:
                if e.partial == b"":
                    logging.info("Receiver stopped: remote closed connection")
                else:
                    logging.error("Error receiving data: %s", e)
                self._aborted.set()
                self._receiver_queue.put(None)
                self._sender_queue.put(None)
                return
            except Exception as e:
                if self._closed:
                    logging.info("Receiver stopped: socket closed")
                else:
                    logging.error("Error receiving data: %s", e)
                self._aborted.set()
                self._receiver_queue.put(None)
                self._sender_queue.put(None)
                return
            if msg_type in (MsgType.QUERY_RESULT, MsgType.ACK, MsgType.QUERY_END):
                self._receiver_queue.put((msg_type, payload))
            else:
                logging.warning(
                    "Unexpected msg_type=%s received from gateway",
                    msg_type,
                )

    def _send_data(self):
        try:
            while True:
                item = self._sender_queue.get()
                if item is None:
                    return
                external.send_msg(self._sock, *item)
        except Exception as e:
            logging.error("Error sending data: %s", e)
            self._disconnect()
        finally:
            self._receiver_queue.put(None)

    def _read_batches(self, csv_path, data_class_type):
        batch_size = self._config.transactions_batch_size if data_class_type == RawTransaction else self._config.accounts_batch_size
        batch = []
        with open(csv_path) as f:
            next(f)
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                batch.append(data_class_type(raw=line))
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def _disconnect(self):
        self._closed = True
        if self._proxy_sock is not None:
            self._proxy_sock.close()
            self._proxy_sock = None
        if self._sock is not None:
            self._sock.close()
            self._sock = None
        self._sender_queue.put(None)
        self._receiver_queue.put(None)

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
