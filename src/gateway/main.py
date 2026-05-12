import logging
import signal
import socket

from gateway.config import Config
from common.socket.safe_socket import SafeSocket
from common.protocol import external
from common.protocol.external import MsgType


class Gateway:
    def __init__(self, config):
        self._config = config
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.bind((config.listen_host, config.listen_port))
        self._server_sock.listen()
        self._closed = False

    def run(self):
        logging.info(f"Gateway listening on port {self._config.listen_port}")

        client_sock_raw, addr = self._server_sock.accept()
        logging.info(f"Client connected from {addr}")
        client_sock = SafeSocket(client_sock_raw)

        total_transactions = 0
        debug_file = None
        if self._config.debug_output_file:
            debug_file = open(self._config.debug_output_file, "w")
            logging.info(f"Writing received transactions to {self._config.debug_output_file}")

        try:
            while True:
                msg_type, payload = external.recv_msg(client_sock)

                if msg_type == MsgType.TRANSACTION_BATCH:
                    if debug_file:
                        for tx in payload:
                            debug_file.write(tx.raw + "\n")
                    total_transactions += len(payload)
                    logging.info(f"Received batch of {len(payload)} transactions (total: {total_transactions})")

                elif msg_type == MsgType.EOF:
                    logging.info(f"EOF received. Total transactions: {total_transactions}")
                    external.send_msg(client_sock, MsgType.ACK)
                    break

                else:
                    logging.warning(f"Unexpected message type: {msg_type}")

        finally:
            if debug_file:
                debug_file.close()
            client_sock.close()
            logging.info("Client connection closed")

    def shutdown(self, signum=None, frame=None):
        if self._closed:
            return
        self._closed = True
        logging.info("Shutdown requested")
        self._server_sock.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = Config.from_env()
    gateway = Gateway(config)
    signal.signal(signal.SIGTERM, gateway.shutdown)
    try:
        gateway.run()
    finally:
        gateway.shutdown()


if __name__ == "__main__":
    main()