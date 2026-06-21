import logging
import threading
import queue

from common.protocol.external import external
from common.protocol.internal import internal
from common.protocol.external.external import MsgType

EXPECTED_QUERY_IDS = (1, 2, 3, 4, 5)
_SENDER_STOP = "__sender_stop__"


class ClientHandler:
    def __init__(self, sock, client_id, gateway_id, router, results_queue):
        self._sock = sock
        self._client_id = client_id
        self._gateway_id = gateway_id
        self._router = router
        self._results_queue = results_queue

    def run(self):
        logging.info("[%s] handler started", self._client_id)
        sender = threading.Thread(target=self._sender_loop, daemon=True)
        sender.start()
        try:
            self._receive_loop()
            sender.join()
        except Exception:
            self._results_queue.put(_SENDER_STOP)
            sender.join()
            raise
        finally:
            self._sock.close()

    def _receive_loop(self):
        got_eof_tx = False
        got_eof_acc = False
        while not (got_eof_tx and got_eof_acc):
            msg_type, payload = external.recv_msg(self._sock)
            got_eof_tx, got_eof_acc = self._dispatch(
                msg_type, payload, got_eof_tx, got_eof_acc
            )

    def _dispatch(self, msg_type, payload, got_eof_tx, got_eof_acc):
        if msg_type == MsgType.TRANSACTION_BATCH:
            records, _, message_id = payload
            self._router.forward_raw_transactions(
                self._client_id, self._gateway_id, records, message_id
            )
            self._results_queue.put(("ack",))
        elif msg_type == MsgType.ACCOUNT_BATCH:
            records, _, message_id = payload
            self._router.forward_raw_accounts(
                self._client_id, self._gateway_id, records, message_id
            )
            self._results_queue.put(("ack",))
        elif msg_type == MsgType.EOF_TRANSACTIONS:
            _, message_id = payload
            logging.info("[%s] EOF_TRANSACTIONS received", self._client_id)
            self._router.forward_eof_transactions(
                self._client_id, self._gateway_id, int(message_id)
            )
            self._results_queue.put(("ack",))
            got_eof_tx = True
        elif msg_type == MsgType.EOF_ACCOUNTS:
            _, message_id = payload
            logging.info("[%s] EOF_ACCOUNTS received", self._client_id)
            self._router.forward_eof_accounts(
                self._client_id, self._gateway_id, int(message_id)
            )
            self._results_queue.put(("ack",))
            got_eof_acc = True
        else:
            logging.warning(
                "[%s] unexpected message type: %s", self._client_id, msg_type
            )
        return got_eof_tx, got_eof_acc

    def _sender_loop(self):
        received_batches = {}
        pending_ends = {}
        finished_queries = 0
        while finished_queries < len(EXPECTED_QUERY_IDS):
            item = self._results_queue.get()
            if item == _SENDER_STOP:
                return
            if item == ("ack",):
                self._send_to_client(MsgType.ACK)
                continue
            msg_type, payload, message_id = item

            if msg_type in EXPECTED_QUERY_IDS:
                query_id = msg_type
                self._send_to_client(
                    MsgType.QUERY_RESULT, query_id, payload, message_id=message_id
                )
                received_batches[query_id] = received_batches.get(query_id, 0) + 1
                if received_batches[query_id] >= pending_ends.get(
                    query_id, float("inf")
                ):
                    self._finalize_query(query_id)
                    pending_ends.pop(query_id)
                    finished_queries += 1
            elif msg_type == internal.MsgType.QUERY_END:
                query_id, message_count = payload
                if received_batches.get(query_id, 0) >= message_count:
                    self._finalize_query(query_id)
                    finished_queries += 1
                else:
                    pending_ends[query_id] = message_count
            else:
                logging.warning(
                    "[%s] unexpected internal msg in results queue: %s",
                    self._client_id,
                    msg_type,
                )

        while True:
            try:
                item = self._results_queue.get_nowait()
                if item == ("ack",):
                    self._send_to_client(MsgType.ACK)
            except queue.Empty:
                break

    def _finalize_query(self, query_id):
        self._send_to_client(MsgType.QUERY_END, query_id)

    def _send_to_client(self, msg_type, *args, message_id=""):
        external.send_msg(self._sock, msg_type, *args, message_id=message_id)
