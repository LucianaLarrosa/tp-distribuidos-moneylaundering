import logging
import os
import uuid

from common.protocol import external
from common.protocol.external import MsgType


class ClientHandler:
    def __init__(self, sock, debug_output_dir=""):
        self._sock = sock
        self._client_id = str(uuid.uuid4())
        self._debug_output_dir = debug_output_dir
        self._msg_count = 0

    def run(self):
        logging.info(f"[{self._client_id}] handler started")

        debug_file_tx = None
        debug_file_acc = None
        if self._debug_output_dir:
            path_tx = os.path.join(
                self._debug_output_dir,
                f"gateway_received_transactions_{self._client_id}.csv",
            )
            debug_file_tx = open(path_tx, "w")

            path_acc = os.path.join(
                self._debug_output_dir,
                f"gateway_received_accounts_{self._client_id}.csv",
            )
            debug_file_acc = open(path_acc, "w")

        try:
            while True:
                msg_type, payload = external.recv_msg(self._sock)

                if msg_type == MsgType.TRANSACTION_BATCH:
                    if debug_file_tx:
                        for tx in payload:
                            debug_file_tx.write(tx.raw + "\n")
                    self._msg_count += len(payload)
                    logging.info(
                        f"[{self._client_id}] batch of {len(payload)} transactions (total: {self._msg_count})"
                    )
                elif msg_type == MsgType.ACCOUNT_BATCH:
                    if debug_file_acc:
                        for acc in payload:
                            debug_file_acc.write(acc.raw + "\n")
                    self._msg_count += len(payload)
                    logging.info(
                        f"[{self._client_id}] batch of {len(payload)} accounts (total: {self._msg_count})"
                    )
                elif msg_type == MsgType.EOF:
                    logging.info(
                        f"[{self._client_id}] EOF received. Total: {self._msg_count}"
                    )
                    external.send_msg(self._sock, MsgType.ACK)
                    break
                else:
                    logging.warning(
                        f"[{self._client_id}] unexpected message type: {msg_type}"
                    )
        finally:
            if debug_file_tx:
                debug_file_tx.close()
            if debug_file_acc:
                debug_file_acc.close()
            self._sock.close()
            logging.info(f"[{self._client_id}] handler finished.")

    def shutdown(self):
        self._sock.close()


def handle_client(sock, debug_output_dir):
    handler = ClientHandler(sock, debug_output_dir)
    handler.run()
