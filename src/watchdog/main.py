import logging
import signal
import subprocess
import threading
import time

from common.health import HealthResponder
from common.protocol.election import election
from common.protocol.election.election import MsgType
from common.protocol.health import health
from common.socket import SafeTCPSocket, SafeUDPSocket, SocketTimeoutError
from config import Config


class Watchdog:
    class Role:
        FOLLOWER = "FOLLOWER"
        LEADER = "LEADER"

    def __init__(self, config):
        self._config = config
        self._closed = False
        self._miss_count = {}
        self._leader_misses = 0

        self._role = Watchdog.Role.FOLLOWER
        self._leader_id = None
        self._election_requested = False

        self._higher_peer_ids = [id for id in config.peers if id > config.watchdog_id]
        self._monitored_hosts = list(config.monitored_nodes) + [
            host
            for peer_id, host in config.peers.items()
            if peer_id != config.watchdog_id
        ]

        self._socket = SafeUDPSocket()
        self._socket.bind(config.ping_pong_host, config.pong_port)

        self._health_responder = HealthResponder(
            config.node_name, config.ping_port, config.ping_pong_host
        )

        self._server_socket = SafeTCPSocket()
        self._server_socket.bind(config.ping_pong_host, config.election_port)
        self._server_socket.listen()

        signal.signal(signal.SIGTERM, lambda *_: self.shutdown())

    def _become_role(self, role, leader_id):
        logging.info("Transitioning to role %s with leader_id %s", role, leader_id)
        self._role = role
        self._leader_id = leader_id
        self._miss_count.clear()
        self._leader_misses = 0
        if role == Watchdog.Role.LEADER:
            self._broadcast_coordinator()

    # --- Election, Client Side (main thread) ---

    def _send_to_peer(self, host, data):
        sock = SafeTCPSocket()
        try:
            sock.connect(host, self._config.election_port)
            election.send_msg(sock, data)
        except OSError as e:
            logging.warning("Failed to reach peer '%s': %s", host, e)
        finally:
            sock.close()

    def _broadcast_coordinator(self):
        for peer_id, host in self._config.peers.items():
            if peer_id == self._config.watchdog_id:
                continue
            self._send_to_peer(
                host, election.serialize_coordinator(self._config.watchdog_id)
            )

    def _run_election(self):
        logging.info("Starting election...")
        self._election_requested = False
        if not self._higher_peer_ids:
            self._become_role(Watchdog.Role.LEADER, self._config.watchdog_id)
            return
        for peer_id in self._higher_peer_ids:
            socket = SafeTCPSocket()
            try:
                socket.connect(self._config.peers[peer_id], self._config.election_port)
                election.send_msg(
                    socket, election.serialize_election(self._config.watchdog_id)
                )
                election.recv_msg(socket, timeout=self._config.election_timeout_seconds)
                self._role = Watchdog.Role.FOLLOWER
                return
            except (SocketTimeoutError, OSError):
                continue
            finally:
                socket.close()
        self._become_role(Watchdog.Role.LEADER, self._config.watchdog_id)

    # --- Election, Server Side (accept thread) ---

    def _handle_election(self, conn):
        election.send_msg(conn, election.serialize_answer(self._config.watchdog_id))
        if self._role == Watchdog.Role.LEADER:
            self._broadcast_coordinator()
        else:
            self._election_requested = True

    def _handle_coordinator(self, leader_id):
        if leader_id > self._config.watchdog_id:
            self._become_role(Watchdog.Role.FOLLOWER, leader_id)
        elif leader_id < self._config.watchdog_id:
            self._election_requested = True

    def _handle_connection(self, client_socket):
        msg_type, node_id = election.recv_msg(client_socket)
        if msg_type == MsgType.ELECTION:
            self._handle_election(client_socket)
        elif msg_type == MsgType.COORDINATOR:
            self._handle_coordinator(node_id)

    def _accept_peer_connections(self):
        while not self._closed:
            try:
                socket, _ = self._server_socket.accept()
            except OSError:
                return
            try:
                self._handle_connection(socket)
            except (SocketTimeoutError, OSError) as e:
                logging.warning("Election connection error: %s", e)
            finally:
                socket.close()

    # --- Ping/Pong + Revive ---

    def _send_ping(self, host):
        self._socket.send(health.serialize_ping(), (host, self._config.ping_port))

    def _send_pings(self):
        for node in self._monitored_hosts:
            try:
                self._send_ping(node)
            except OSError as e:
                logging.warning("Failed to ping '%s': %s", node, e)

    def _collect_pongs_until(self, timeout):
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                data, _ = self._socket.recv(timeout=remaining)
            except (SocketTimeoutError, OSError):
                return
            try:
                node_name = health.deserialize_pong(data)
                if node_name:
                    yield node_name
            except Exception as e:
                logging.warning("Ignoring malformed pong: %s", e)

    def _collect_pongs(self):
        return set(self._collect_pongs_until(self._config.ping_timeout_seconds))

    def _revive(self, node_name):
        result = subprocess.run(
            ["docker", "start", node_name], capture_output=True, text=True
        )
        if result.returncode != 0:
            logging.error("Failed to revive '%s': %s", node_name, result.stderr.strip())
        else:
            logging.info("Node '%s' revived", node_name)

    def _process_results(self, responded):
        for node in self._monitored_hosts:
            if node in responded:
                self._miss_count[node] = 0
                continue
            self._miss_count[node] = self._miss_count.get(node, 0) + 1
            if self._miss_count[node] >= self._config.max_retries:
                logging.warning(
                    "Node '%s' presumed dead after %d missed pongs, reviving...",
                    node,
                    self._miss_count[node],
                )
                self._revive(node)
                self._miss_count[node] = 0

    def _probe_leader(self):
        leader_host = self._config.peers.get(self._leader_id)
        if leader_host is None:
            return False
        try:
            self._send_ping(leader_host)
        except OSError:
            return False
        return leader_host in self._collect_pongs_until(
            self._config.ping_timeout_seconds
        )

    # --- Main loop ---

    def _run(self):
        while not self._closed:
            if self._election_requested:
                self._run_election()
                continue
            if self._role == Watchdog.Role.LEADER:
                self._send_pings()
                responded = self._collect_pongs()
                self._process_results(responded)
            else:
                if self._probe_leader():
                    self._leader_misses = 0
                else:
                    self._leader_misses += 1
                    if self._leader_misses >= self._config.leader_probe_miss_threshold:
                        logging.warning(
                            "Leader watchdog %s unreachable, starting election",
                            self._leader_id,
                        )
                        self._leader_misses = 0
                        self._run_election()
                        continue
            time.sleep(self._config.check_interval_seconds)

    def start(self):
        logging.info("Starting watchdog...")
        self._health_responder.start()
        threading.Thread(target=self._accept_peer_connections, daemon=True).start()
        self._run_election()
        self._run()

    def shutdown(self):
        if self._closed:
            return
        self._closed = True
        logging.info("Shutting down watchdog...")
        self._health_responder.stop()
        self._server_socket.close()
        self._socket.close()


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
