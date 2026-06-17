import logging
import queue
import threading
import time

from common.protocol.election import election
from common.socket import IncompleteReadError, SafeTCPSocket


class PeerHandler:
    _INITIAL_RECONNECT_DELAY = 1.0
    _MAX_RECONNECT_DELAY = 30.0
    _BACKOFF_FACTOR = 2.0

    class EventType:
        MESSAGE = "MESSAGE"
        DISCONNECT = "DISCONNECT"

    def __init__(self, peer_id, events):
        self._peer_id = peer_id
        self._events = events
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

    def _receiver_loop(self, sock):
        while True:
            try:
                msg_type, node_id = election.recv_msg(sock)
            except (OSError, IncompleteReadError) as e:
                logging.warning("Lost connection to peer %d: %s", self._peer_id, e)
                self._events.put((self.EventType.DISCONNECT, self._peer_id))
                self._send_queue.put(None)
                return
            self._events.put(
                (self.EventType.MESSAGE, (self._peer_id, msg_type, node_id))
            )

    def _run_session(self, sock):
        self._socket = sock
        sender = threading.Thread(target=self._sender_loop, args=(sock,), daemon=True)
        sender.start()
        self._receiver_loop(sock)
        self._socket = None
        sock.close()
        sender.join()

    def _run(self, host, port, my_id):
        delay = self._INITIAL_RECONNECT_DELAY
        while not self._closed:
            try:
                sock = SafeTCPSocket()
                sock.connect(host, port)
                delay = self._INITIAL_RECONNECT_DELAY
                election.send_peer_id(sock, my_id)
                self._run_session(sock)
            except OSError as e:
                logging.warning("Could not connect to peer %d: %s", self._peer_id, e)
            if not self._closed:
                logging.info("Reconnecting to peer %d in %.1fs", self._peer_id, delay)
                time.sleep(delay)
                delay = min(delay * self._BACKOFF_FACTOR, self._MAX_RECONNECT_DELAY)

    def send(self, data):
        self._send_queue.put(data)

    def accept(self, sock):
        """
        Start a thread to manage a newly accepted connection to a peer, running until the connection is lost or the handler is closed.
        """
        threading.Thread(target=self._run_session, args=(sock,), daemon=True).start()

    def connect(self, host, port, my_id):
        """
        Start a thread to manage the connection to a peer, attempting to connect immediately, running and then retrying with a delay if it fails, until the handler is closed.
        """
        threading.Thread(
            target=self._run,
            args=(host, port, my_id),
            daemon=True,
        ).start()

    def close(self):
        self._closed = True
        if self._socket is not None:
            self._socket.close()
