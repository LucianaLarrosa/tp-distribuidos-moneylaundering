import logging
import queue
import threading
import time

from common.protocol.election import election
from common.socket import IncompleteReadError, SafeTCPSocket


class PeerHandler:
    _RECONNECT_DELAY = 1.0

    def __init__(self, peer_id):
        self._peer_id = peer_id
        self._send_queue = queue.Queue()
        self._socket = None
        self._closed = False

    def _sender_loop(self, sock):
        while True:
            data = self._send_queue.get()
            if data is None:
                break
            try:
                election.send_msg(sock, data)
            except OSError as e:
                logging.warning("Sender for peer %d failed: %s", self._peer_id, e)
                sock.close()
                break

    def _receiver_loop(self, sock, on_message, on_disconnect):
        while True:
            try:
                msg_type, node_id = election.recv_msg(sock)
            except (OSError, IncompleteReadError) as e:
                logging.warning("Lost connection to peer %d: %s", self._peer_id, e)
                on_disconnect(self._peer_id)
                self._send_queue.put(None)
                return
            on_message(self._peer_id, msg_type, node_id)

    def _run(self, sock, on_message, on_disconnect):
        self._socket = sock
        sender = threading.Thread(target=self._sender_loop, args=(sock,), daemon=True)
        sender.start()
        self._receiver_loop(sock, on_message, on_disconnect)
        self._socket = None
        sock.close()
        sender.join()

    def _monitor_peer_status(self, host, port, my_id, on_message, on_disconnect):
        while not self._closed:
            try:
                sock = SafeTCPSocket()
                sock.connect(host, port)
                election.send_peer_id(sock, my_id)
                self._run(sock, on_message, on_disconnect)
            except OSError as e:
                logging.warning("Could not connect to peer %d: %s", self._peer_id, e)
            if not self._closed:
                time.sleep(self._RECONNECT_DELAY)

    def send(self, data):
        self._send_queue.put(data)

    def accept(self, sock, on_message, on_disconnect):
        """
        Start a thread to manage a newly accepted connection to a peer, running until the connection is lost or the handler is closed.
        """
        threading.Thread(
            target=self._run, args=(sock, on_message, on_disconnect), daemon=True
        ).start()

    def connect(self, host, port, my_id, on_message, on_disconnect):
        """
        Start a thread to manage the connection to a peer, attempting to connect immediately, running and then retrying with a delay if it fails, until the handler is closed.
        """
        threading.Thread(
            target=self._monitor_peer_status,
            args=(host, port, my_id, on_message, on_disconnect),
            daemon=True,
        ).start()

    def close(self):
        self._closed = True
        if self._socket is not None:
            self._socket.close()
