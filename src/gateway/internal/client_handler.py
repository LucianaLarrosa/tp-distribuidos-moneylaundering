import logging
import uuid

from common.protocol import external
from common.protocol.external import MsgType


class ClientHandler:
    def __init__(self, sock, gateway_id, router):
        self._sock = sock
        self._client_id = str(uuid.uuid4())
        self._gateway_id = gateway_id
        self._router = router
        self._tx_batch_count = 0
        self._acc_batch_count = 0

    def run(self):
        logging.info(f"[{self._client_id}] handler started")

        got_eof_tx = False
        got_eof_acc = False
        try:
            while not (got_eof_tx and got_eof_acc):
                msg_type, payload = external.recv_msg(self._sock)

                if msg_type == MsgType.TRANSACTION_BATCH:
                    self._router.forward_raw_transactions(
                        self._client_id, self._gateway_id, payload
                    )
                    self._tx_batch_count += 1
                    logging.info(
                        f"[{self._client_id}] tx batch #{self._tx_batch_count} ({len(payload)} items) forwarded"
                    )
                elif msg_type == MsgType.ACCOUNT_BATCH:
                    self._router.forward_raw_accounts(
                        self._client_id, self._gateway_id, payload
                    )
                    self._acc_batch_count += 1
                    logging.info(
                        f"[{self._client_id}] acc batch #{self._acc_batch_count} ({len(payload)} items) forwarded"
                    )
                elif msg_type == MsgType.EOF_TRANSACTIONS:
                    logging.info(f"[{self._client_id}] EOF_TRANSACTIONS received")
                    self._router.forward_eof_transactions(
                        self._client_id, self._gateway_id, self._tx_batch_count
                    )
                    external.send_msg(self._sock, MsgType.ACK)
                    got_eof_tx = True
                elif msg_type == MsgType.EOF_ACCOUNTS:
                    logging.info(f"[{self._client_id}] EOF_ACCOUNTS received")
                    self._router.forward_eof_accounts(
                        self._client_id, self._gateway_id, self._acc_batch_count
                    )
                    external.send_msg(self._sock, MsgType.ACK)
                    got_eof_acc = True
                else:
                    logging.warning(
                        f"[{self._client_id}] unexpected message type: {msg_type}"
                    )
            logging.info(
                f"[{self._client_id}] all EOFs received. "
                f"tx_batches={self._tx_batch_count} acc_batches={self._acc_batch_count}"
            )
        finally:
            self._sock.close()
            logging.info(f"[{self._client_id}] handler finished.")

