import logging
import queue
import signal
import threading
import time

from common.health import HealthResponder
from common.protocol.election import election
from common.protocol.election.election import MsgType
from common.socket import SafeTCPSocket
from config import Config
from health_monitor import HealthMonitor
from peer_handler import PeerHandler


class Watchdog:
    class Role:
        FOLLOWER = "FOLLOWER"
        LEADER = "LEADER"

    class Phase:
        IDLE = "IDLE"
        AWAITING_ANSWER = "AWAITING_ANSWER"
        AWAITING_COORDINATOR = "AWAITING_COORDINATOR"

    def __init__(self, config):
        self._config = config
        self._closed = False

        self._role = Watchdog.Role.FOLLOWER
        self._leader_id = None
        self._phase = Watchdog.Phase.IDLE
        self._deadline = 0.0
        self._events = queue.Queue()

        self._peer_handlers = {
            peer_id: PeerHandler(peer_id, self._events)
            for peer_id in config.peers
            if peer_id != config.watchdog_id
        }
        self._higher_peer_ids = [id for id in config.peers if id > config.watchdog_id]
        self._server_socket = SafeTCPSocket()
        self._server_socket.bind(config.ping_pong_host, config.election_port)
        self._server_socket.listen()

        monitored_hosts = list(config.monitored_nodes) + [
            host for id, host in config.peers.items() if id != config.watchdog_id
        ]
        self._health_monitor = HealthMonitor(
            monitored_hosts=monitored_hosts,
            host=config.ping_pong_host,
            pong_port=config.pong_port,
            ping_port=config.ping_port,
            ping_timeout=config.ping_timeout_seconds,
            check_interval=config.check_interval_seconds,
            max_retries=config.max_retries,
        )

        self._health_responder = HealthResponder(
            config.node_name, config.ping_port, config.ping_pong_host
        )

        signal.signal(signal.SIGTERM, lambda *_: self.shutdown())

    # --- Peer Management ---

    def _accept_lower_peer_connections(self):
        while not self._closed:
            try:
                sock, _ = self._server_socket.accept()
            except OSError:
                return
            try:
                peer_id = election.recv_peer_id(sock)
            except OSError as e:
                logging.warning("Failed to read peer ID: %s", e)
                sock.close()
                continue
            logging.info("Accepted connection from peer %d", peer_id)
            self._peer_handlers[peer_id].accept(sock)

    def _connect_to_higher_peers(self):
        for peer_id, host in self._config.peers.items():
            if peer_id > self._config.watchdog_id:
                self._peer_handlers[peer_id].connect(
                    host,
                    self._config.election_port,
                    self._config.watchdog_id,
                )

    # --- Election ---

    def _send_to_peer(self, peer_id, data):
        self._peer_handlers[peer_id].send(data)

    def _send_to_peers(self, peer_ids, data):
        for peer_id in peer_ids:
            self._send_to_peer(peer_id, data)

    def _broadcast_coordinator(self):
        self._send_to_peers(
            self._peer_handlers.keys(),
            election.serialize_coordinator(self._config.watchdog_id),
        )

    def _become_role(self, new_role, leader_id=None):
        if leader_id is None:
            leader_id = self._config.watchdog_id
        logging.info("Becoming %s (leader=%d)", new_role, leader_id)
        self._role = new_role
        self._leader_id = leader_id
        self._phase = Watchdog.Phase.IDLE
        if new_role == Watchdog.Role.LEADER:
            self._broadcast_coordinator()
            self._health_monitor.start()
        else:
            self._health_monitor.stop()

    def _enter_phase(self, phase):
        self._phase = phase
        self._deadline = time.monotonic() + self._config.election_timeout_seconds

    def _start_election(self):
        if self._closed:
            return
        logging.info("Starting election...")
        self._leader_id = None
        if not self._higher_peer_ids:
            self._become_role(Watchdog.Role.LEADER)
            return
        self._send_to_peers(
            self._higher_peer_ids, election.serialize_election(self._config.watchdog_id)
        )
        self._enter_phase(Watchdog.Phase.AWAITING_ANSWER)

    def _handle_election_timeout(self):
        if self._phase == Watchdog.Phase.AWAITING_ANSWER:
            self._become_role(Watchdog.Role.LEADER)
        elif self._phase == Watchdog.Phase.AWAITING_COORDINATOR:
            self._start_election()

    def _handle_election_message(self, peer_id):
        self._send_to_peer(peer_id, election.serialize_answer(self._config.watchdog_id))
        if self._role == Watchdog.Role.LEADER:
            self._broadcast_coordinator()
        else:
            self._start_election()

    def _handle_coordinator_message(self, leader_id):
        if leader_id < self._config.watchdog_id:
            self._start_election()
        else:
            self._become_role(Watchdog.Role.FOLLOWER, leader_id)

    def _handle_message(self, peer_id, msg_type, node_id):
        """
        Bully algorithm: if an election message is received, answer and trigger an election if not already a leader; if an answer message is received, enter to the awaiting coordinator phase; if a coordinator message is received, become a follower if the leader ID is higher, or trigger an election if it's lower.
        """
        if msg_type == MsgType.ELECTION:
            self._handle_election_message(peer_id)
        elif msg_type == MsgType.ANSWER:
            if self._phase == Watchdog.Phase.AWAITING_ANSWER:
                self._enter_phase(Watchdog.Phase.AWAITING_COORDINATOR)
        elif msg_type == MsgType.COORDINATOR:
            self._handle_coordinator_message(node_id)

    def _handle_disconnect(self, peer_id):
        if peer_id == self._leader_id:
            logging.warning("Leader %d disconnected, triggering election", peer_id)
            self._start_election()

    # --- Event loop ---

    def _seconds_until_deadline(self):
        if self._phase == Watchdog.Phase.IDLE:
            return None
        return max(0.0, self._deadline - time.monotonic())

    def _event_loop(self):
        self._start_election()
        while not self._closed:
            try:
                event = self._events.get(timeout=self._seconds_until_deadline())
            except queue.Empty:
                self._handle_election_timeout()
                continue
            if event is None:
                break
            event_type, event_data = event
            if event_type == PeerHandler.EventType.MESSAGE:
                self._handle_message(*event_data)
            elif event_type == PeerHandler.EventType.DISCONNECT:
                self._handle_disconnect(event_data)

    # --- Lifecycle ---

    def start(self):
        logging.info("Starting watchdog %d...", self._config.watchdog_id)
        self._health_responder.start()
        threading.Thread(
            target=self._accept_lower_peer_connections, daemon=True
        ).start()
        self._connect_to_higher_peers()
        self._event_loop()

    def shutdown(self):
        if self._closed:
            return
        self._closed = True
        logging.info("Shutting down watchdog %d...", self._config.watchdog_id)
        self._events.put(None)
        self._health_responder.stop()
        self._health_monitor.close()
        self._server_socket.close()
        for handler in self._peer_handlers.values():
            handler.close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [Watchdog] %(levelname)s %(message)s",
    )
    config = Config()
    watchdog = Watchdog(config)
    try:
        watchdog.start()
    except Exception as e:
        logging.error("Error during Watchdog execution: %s", e)
    finally:
        watchdog.shutdown()


if __name__ == "__main__":
    main()
