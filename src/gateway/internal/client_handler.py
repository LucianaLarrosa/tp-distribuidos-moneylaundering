import logging
import os
import uuid

from common.protocol import external
from common.protocol.external import MsgType


class ClientHandler:
    def __init__(self, sock, debug_output_dir = ""):
        self._sock = sock
        self._client_id = str(uuid.uuid4())
        self._debug_output_dir = debug_output_dir
        self._msg_count = 0

    def run(self):
        logging.info(f"[{self._client_id}] handler started")

        debug_file = None
        if self._debug_output_dir:
            path = os.path.join(self._debug_output_dir, f"gateway_received_{self._client_id}.csv")
            debug_file = open(path, "w")
            logging.info(f"[{self._client_id}] writing transactions to {path}")

        try:
            while True:
                msg_type, payload = external.recv_msg(self._sock)

                if msg_type == MsgType.TRANSACTION_BATCH:
                    if debug_file:
                        for tx in payload:
                            debug_file.write(tx.raw + "\n")
                    self._msg_count += len(payload)
                    logging.info(f"[{self._client_id}] batch of {len(payload)} transactions (total: {self._msg_count})")

                elif msg_type == MsgType.EOF:
                    logging.info(f"[{self._client_id}] EOF received. Total: {self._msg_count}")
                    external.send_msg(self._sock, MsgType.ACK)
                    break

                else:
                    logging.warning(f"[{self._client_id}] unexpected message type: {msg_type}")

        finally:
            if debug_file:
                debug_file.close()
            self._sock.close()
            logging.info(f"[{self._client_id}] handler finished")

    def shutdown(self):
        self._sock.close()


def handle_client(sock, debug_output_dir):
    handler = ClientHandler(sock, debug_output_dir)
    handler.run()
