import logging
import threading
import queue

from common.protocol.external import external
from common.protocol.internal import internal
from common.protocol.external.external import MsgType

EXPECTED_QUERY_IDS = (1, 2, 3, 4, 5)
_SENDER_STOP = "__sender_stop__"


class ClientHandler:
    def __init__(self, sock, client_id, router, results):
        self._sock = sock
        self._client_id = client_id
        self._router = router
        self._results = results
        self._queue = queue.Queue()
        self._pending_acks = {}
        self._ack_lock = threading.Lock()
        self._next_delivery_id = 0

    def run(self):
        logging.info("[%s] handler started", self._client_id)
        result_receiver = threading.Thread(
            target=self._result_receiver_loop, daemon=True
        )
        sender = threading.Thread(target=self._sender_loop, daemon=True)
        result_receiver.start()
        sender.start()
        try:
            self._receive_loop()
        except Exception:
            logging.info("[%s] client disconnected", self._client_id)
        finally:
            self._queue.put(_SENDER_STOP)
            sender.join()
            try:
                self._results.stop_consuming_threadsafe()
            except Exception:
                pass
            result_receiver.join()
            self._sock.close()

    def _receive_loop(self):
        while True:
            msg_type, payload = external.recv_msg(self._sock)
            self._dispatch(msg_type, payload)

    def _dispatch(self, msg_type, payload):
        if msg_type == MsgType.TRANSACTION_BATCH:
            records, _, message_id = payload
            self._router.forward_raw_transactions(
                self._client_id, records, message_id
            )
            self._queue.put(("ack",))
        elif msg_type == MsgType.ACCOUNT_BATCH:
            records, _, message_id = payload
            self._router.forward_raw_accounts(
                self._client_id, records, message_id
            )
            self._queue.put(("ack",))
        elif msg_type == MsgType.EOF_TRANSACTIONS:
            _, message_id = payload
            logging.info("[%s] EOF_TRANSACTIONS received", self._client_id)
            self._router.forward_eof_transactions(
                self._client_id, int(message_id)
            )
            self._queue.put(("ack",))
        elif msg_type == MsgType.EOF_ACCOUNTS:
            _, message_id = payload
            logging.info("[%s] EOF_ACCOUNTS received", self._client_id)
            self._router.forward_eof_accounts(
                self._client_id, int(message_id)
            )
            self._queue.put(("ack",))
        elif msg_type == MsgType.RESULT_ACK:
            self._on_result_ack(payload)
        else:
            logging.warning(
                "[%s] unexpected message type: %s", self._client_id, msg_type
            )

    def _result_receiver_loop(self):
        try:
            self._results.start_consuming(self._on_result)
        except Exception as e:
            logging.warning(
                "[%s] results consumer stopped: %s", self._client_id, e
            )
        finally:
            try:
                self._results.close()
            except Exception:
                pass

    def _on_result(self, body, ack, _nack):
        msg_type, _, payload, message_id = internal.deserialize_msg(body)
        self._queue.put((msg_type, payload, message_id, ack))

    def _sender_loop(self):
        while True:
            item = self._queue.get()
            if item == _SENDER_STOP:
                return
            if item == ("ack",):
                self._send_to_client(MsgType.ACK)
                continue
            msg_type, payload, message_id, ack = item

            if msg_type in EXPECTED_QUERY_IDS:
                delivery_id = self._register_pending_ack(ack)
                self._send_to_client(
                    MsgType.QUERY_RESULT,
                    msg_type,
                    payload,
                    message_id=message_id,
                    delivery_id=delivery_id,
                )
            elif msg_type == internal.MsgType.QUERY_END:
                delivery_id = self._register_pending_ack(ack)
                query_id, message_count = payload
                self._send_to_client(
                    MsgType.QUERY_END,
                    query_id,
                    message_count,
                    delivery_id=delivery_id,
                )
            else:
                logging.warning(
                    "[%s] unexpected internal msg in results queue: %s",
                    self._client_id,
                    msg_type,
                )
                self._ack_to_rabbit(ack)

    def _register_pending_ack(self, ack):
        with self._ack_lock:
            self._next_delivery_id += 1
            delivery_id = self._next_delivery_id
            self._pending_acks[delivery_id] = ack
        return delivery_id

    def _on_result_ack(self, delivery_id):
        with self._ack_lock:
            ack = self._pending_acks.pop(delivery_id, None)
        if ack is not None:
            self._ack_to_rabbit(ack)

    def _ack_to_rabbit(self, ack):
        self._results.connection.add_callback_threadsafe(ack)

    def _send_to_client(self, msg_type, *args, message_id="", delivery_id=0):
        external.send_msg(
            self._sock, msg_type, *args, message_id=message_id, delivery_id=delivery_id
        )
