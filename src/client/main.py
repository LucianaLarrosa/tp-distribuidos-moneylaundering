import csv
import logging
import os
import signal
import threading
import queue
import uuid
from dataclasses import asdict

from client.config import Config
from common.socket.safe_socket import SafeTCPSocket, IncompleteReadError
from common.models.raw_transaction import RawTransaction
from common.models.raw_account import RawAccount
from common.protocol.external import external
from common.protocol.external.external import MsgType


class Client:
    _INITIAL_RECONNECT_DELAY = 1.0
    _MAX_RECONNECT_DELAY = 30.0
    _BACKOFF_FACTOR = 2.0

    def __init__(self, config):
        self._config = config
        self._client_id = str(uuid.uuid4())
        self._sock = None
        self._proxy_sock = None
        self._receiver_queue = None
        self._sender_queue = None
        self._aborted = None
        self._closed = False
        self._shutdown_event = threading.Event()
        self._message_iter = None
        self._pending_msg = None
        self._seen_results = set()  # (query_id, message_id)

    def run(self):
        try:
            gateway_host, gateway_port = self._resolve_gateway()
        except OSError:
            if self._closed:
                return
            raise
        delay = self._INITIAL_RECONNECT_DELAY
        while not self._closed:
            try:
                sock = SafeTCPSocket()
                sock.connect(gateway_host, gateway_port)
                delay = self._INITIAL_RECONNECT_DELAY
                logging.info("Connected to gateway %s:%s", gateway_host, gateway_port)
                if self._run_session(sock):
                    logging.info("Shutdown: All expected query results received")
                    return
            except OSError as e:
                logging.warning(
                    "Could not connect to gateway %s:%s: %s",
                    gateway_host,
                    gateway_port,
                    e,
                )
            if not self._closed:
                logging.info("Reconnecting to gateway in %.1fs", delay)
                self._shutdown_event.wait(delay)
                delay = min(delay * self._BACKOFF_FACTOR, self._MAX_RECONNECT_DELAY)

    def _run_session(self, sock):
        self._sock = sock
        self._aborted = False
        external.send_msg(sock, MsgType.ANNOUNCE, client_id=self._client_id)
        self._receiver_queue = queue.Queue()
        self._sender_queue = queue.Queue()
        receiver_thread = threading.Thread(target=self._receive_data, daemon=True)
        sender_thread = threading.Thread(target=self._send_data, daemon=True)
        receiver_thread.start()
        sender_thread.start()
        try:
            return self._orchestrate()
        finally:
            sock.close()
            self._sock = None
            receiver_thread.join()
            sender_thread.join()

    def _orchestrate(self):
        logging.info("Starting data transmission")
        pending = set(self._config.expected_query_ids)
        writers, totals, files = self._open_result_files(pending)
        try:
            self._produce_and_wait_acks(pending, writers, totals)
            if not self._aborted:
                self._consume_remaining(pending, writers, totals)
        finally:
            for f in files.values():
                f.close()
            self._sender_queue.put(None)
        return not pending

    def _produce_and_wait_acks(self, pending, writers, totals):
        if self._message_iter is None:
            self._message_iter = self._messages_to_send()
        while not (self._aborted or self._closed):
            if self._pending_msg is None:
                try:
                    self._pending_msg = next(self._message_iter)
                except StopIteration:
                    return
            msg_type, payload, batch_index = self._pending_msg
            self._sender_queue.put((msg_type, payload, batch_index))
            if not self._wait_ack(pending, writers, totals):
                return
            self._pending_msg = None

    def _messages_to_send(self):
        tx_count = 0
        for batch in self._read_batches(
            self._config.input_csv_transactions, RawTransaction
        ):
            yield MsgType.TRANSACTION_BATCH, batch, tx_count
            tx_count += 1
        yield MsgType.EOF_TRANSACTIONS, None, tx_count
        acc_count = 0
        for batch in self._read_batches(self._config.input_csv_accounts, RawAccount):
            yield MsgType.ACCOUNT_BATCH, batch, acc_count
            acc_count += 1
        yield MsgType.EOF_ACCOUNTS, None, acc_count

    def _wait_ack(self, pending, writers, totals):
        while not self._closed:
            item = self._receiver_queue.get()
            if item is None:
                return False
            msg_type, payload = item
            if msg_type == MsgType.ACK:
                return True
            elif msg_type == MsgType.QUERY_RESULT:
                self._handle_query_result(payload, writers, totals)
            elif msg_type == MsgType.QUERY_END:
                self._handle_query_end(payload, pending, totals)
            else:
                logging.warning("Unexpected msg_type=%s in orchestrator", msg_type)
        return False

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
                    f"q{qid}_client_{self._config.client_name}.csv",
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
        query_id, records, message_id = payload
        if message_id:
            key = (query_id, message_id)
            if key in self._seen_results:
                return
            self._seen_results.add(key)
        for record in records:
            writers[query_id].writerow(asdict(record).values())
        totals[query_id] += len(records)

    def _handle_query_end(self, payload, pending, totals):
        query_id = payload
        pending.discard(query_id)
        logging.info("Q%s ended (%s records total)", query_id, totals[query_id])

    def _resolve_gateway(self):
        logging.info("Connecting to proxy")
        self._proxy_sock = SafeTCPSocket()
        self._proxy_sock.connect(self._config.proxy_host, self._config.proxy_port)

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
                self._aborted = True
                self._receiver_queue.put(None)
                self._sender_queue.put(None)
                return
            except Exception as e:
                if self._closed:
                    logging.info("Receiver stopped: socket closed")
                else:
                    logging.error("Error receiving data: %s", e)
                self._aborted = True
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
                msg_type, payload, batch_index = item
                if payload is None:
                    external.send_msg(
                        self._sock,
                        msg_type,
                        client_id=self._client_id,
                        message_id=str(batch_index),
                    )
                else:
                    external.send_msg(
                        self._sock,
                        msg_type,
                        payload,
                        client_id=self._client_id,
                        message_id=str(batch_index),
                    )
        except Exception as e:
            if not self._closed:
                logging.warning("Send failed, aborting session: %s", e)
            self._aborted = True
        finally:
            self._receiver_queue.put(None)

    def _read_batches(self, csv_path, data_class_type):
        batch_size = (
            self._config.transactions_batch_size
            if data_class_type == RawTransaction
            else self._config.accounts_batch_size
        )
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
        if self._sender_queue is not None:
            self._sender_queue.put(None)
        if self._receiver_queue is not None:
            self._receiver_queue.put(None)
        self._shutdown_event.set()

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
