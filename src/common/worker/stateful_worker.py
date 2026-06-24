import math
import threading
from abc import abstractmethod
from collections import defaultdict

from common.communication.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.communication.protocol import internal
from common.idempotency.ids import (
    flush_id,
    ring_id,
    ring_seq_of,
    RING_PHASE_COUNT,
    RING_PHASE_FLUSH,
    RING_PHASE_CLEANUP,
)
from common.models.eof import EOF, RingEOF, CLEANUP_EXPECTED_COUNT
from common.worker.sharding import hash_of
from common.worker.worker import Worker, MAIN_CHANNEL

CONTROL_CHANNEL = "control"


class StatefulWorker(Worker):
    def __init__(self, config):
        super().__init__(config)
        self._processed_counts = (
            {}
        )  # client_id -> processed_count | actual total processed count
        self._partial_processed_count = (
            {}
        )  # client_id -> partial_processed_count | previous process count sent to ring
        self._processed_counts_lock = threading.Lock()
        self._sent_count = {}  # client_id -> sent_count | actual total sent count
        self._partial_sent_count = (
            {}
        )  # client_id -> partial_sent_count | previous sent count sent to ring
        self._sent_count_lock = threading.Lock()
        self._control_thread = None

        self._input_control_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=self._rabbitmq_host,
            exchange_name=self._control_exchange_name,
            routing_keys=[self._get_ring_routing_key(self._node_id)],
            queue_name=f"{self._control_exchange_name}.{self._node_id}",
        )
        self._output_control_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=self._rabbitmq_host,
            exchange_name=self._control_exchange_name,
            routing_keys=[],
        )
        self._control_output_control_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=self._rabbitmq_host,
            exchange_name=self._control_exchange_name,
            routing_keys=[],
        )

    @property
    @abstractmethod
    def _rabbitmq_host(self):
        pass

    @property
    @abstractmethod
    def _control_exchange_name(self):
        pass

    @property
    @abstractmethod
    def _node_prefix(self):
        pass

    @property
    @abstractmethod
    def _node_id(self):
        pass

    @property
    @abstractmethod
    def _ring_size(self):
        pass

    @abstractmethod
    def _flush_data(self, client_id):
        pass

    @property
    def _input_control_middleware(self):
        return self._input_control_exchange

    @property
    def _output_control_middleware(self):
        return self._output_control_exchange

    @property
    def _control_output_control_middleware(self):
        return self._control_output_control_exchange

    def _get_ring_routing_key(self, node_id):
        return f"{self._node_prefix}{node_id}"

    def _get_total_count(self, key, count_dict, partial_dict, lock, current_total):
        with lock:
            count = count_dict.get(key, 0)
            partial = partial_dict.get(key, 0)
            partial_dict[key] = count
        return current_total + count - partial

    def _increment_processed_count(self, client_id):
        with self._processed_counts_lock:
            self._processed_counts[client_id] = (
                self._processed_counts.get(client_id, 0) + 1
            )

    def _get_total_sent_count(self, client_id, current_total):
        """
        Get the total sent count for the given client_id. A stateful-coordinated worker tracks sent counts, so this method returns the calculated total sent count.
        """
        return self._get_total_count(
            client_id,
            self._sent_count,
            self._partial_sent_count,
            self._sent_count_lock,
            current_total,
        )

    def _get_final_eof_count(self, ring_eof):
        """
        Get the final EOF count to send to the output middleware when this node is the coordinator. In a stateful-coordinated worker, the final EOF count is the total sent count from the RING_EOF message.
        """
        return ring_eof.total_sent_count

    def _increment_sent_count(self, client_id):
        with self._sent_count_lock:
            self._sent_count[client_id] = self._sent_count.get(client_id, 0) + 1

    def _update_ring_eof(self, client_id, ring_eof):
        """
        Update the RING_EOF message with the processed and sent counts and determine if this node should be the coordinator.
        """
        total_processed_count = self._get_total_count(
            client_id,
            self._processed_counts,
            self._partial_processed_count,
            self._processed_counts_lock,
            ring_eof.total_processed_count,
        )
        coordinator_id = (
            self._node_id if total_processed_count >= ring_eof.expected_count else None
        )
        return RingEOF(
            expected_count=ring_eof.expected_count,
            total_processed_count=total_processed_count,
            coordinator_id=coordinator_id,
            total_sent_count=self._get_total_sent_count(
                client_id, ring_eof.total_sent_count or 0
            ),
        )

    def _handle_control_eof_message(
        self, client_id, ring_eof, in_message_id="", output_exchange=None
    ):
        """
        Handle a RING_EOF message by either forwarding it to the next node in the ring, flushing data and sending an EOF to the output middleware if this node is the coordinator.
        """
        if output_exchange is None:
            output_exchange = self._control_output_control_middleware

        cleanup = ring_eof.expected_count == CLEANUP_EXPECTED_COUNT
        action_phase = RING_PHASE_CLEANUP if cleanup else RING_PHASE_FLUSH

        if ring_eof.coordinator_id is None:
            ring_eof = self._update_ring_eof(client_id, ring_eof)
            if ring_eof.coordinator_id == self._node_id:
                out_seq = ring_seq_of(in_message_id) + 1 if cleanup else 0
                out_message_id = ring_id(client_id, action_phase, out_seq)
            else:
                out_message_id = ring_id(
                    client_id,
                    RING_PHASE_COUNT,
                    ring_seq_of(in_message_id) + 1,
                )
        else:
            if cleanup:
                self._cleanup_flow(client_id)
            else:
                self._flush_data(client_id)
                ring_eof.total_sent_count = self._get_total_sent_count(
                    client_id, ring_eof.total_sent_count or 0
                )
            if ring_eof.coordinator_id == self._node_id:
                final_eof = (
                    EOF(CLEANUP_EXPECTED_COUNT)
                    if cleanup
                    else EOF(self._get_final_eof_count(ring_eof))
                )
                self._send_final_eof(client_id, final_eof)
                return
            out_message_id = ring_id(
                client_id, action_phase, ring_seq_of(in_message_id) + 1
            )
        output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.RING_EOF,
                client_id,
                ring_eof,
                message_id=out_message_id,
            ),
            routing_key=self._get_ring_routing_key(self._get_next_node_id()),
        )

    def _handle_control_message(self, message, ack, nack):
        try:
            _, client_id, ring_eof, message_id = internal.deserialize_msg(message)
            with self._state_lock:
                seen = self._seen.setdefault((CONTROL_CHANNEL, client_id), set())
                if message_id in seen:
                    ack()
                    return
                self._handle_control_eof_message(
                    client_id, ring_eof, message_id
                )
                seen.add(message_id)
                self._state_store.append(
                    {
                        "ch": CONTROL_CHANNEL,
                        "mid": message_id,
                        "c": client_id,
                        "ring": self._control_state_snapshot(client_id),
                    }
                )
                self._note_append()
            ack()
        except Exception:
            nack()
            raise

    def _control_state_snapshot(self, client_id):
        snapshot = {"partial_processed": self._partial_processed_count.get(client_id, 0)}
        snapshot["sent_count"] = self._sent_count.get(client_id, 0)
        snapshot["partial_sent"] = self._partial_sent_count.get(client_id, 0)
        return snapshot

    def _restore_control_state(self, client_id, snapshot):
        self._partial_processed_count[client_id] = snapshot["partial_processed"]
        self._sent_count[client_id] = snapshot["sent_count"]
        self._partial_sent_count[client_id] = snapshot["partial_sent"]

    def _cleanup_state(self, client_id):
        super()._cleanup_state(client_id)
        with self._processed_counts_lock:
            self._processed_counts.pop(client_id, None)
            self._partial_processed_count.pop(client_id, None)
        with self._sent_count_lock:
            self._sent_count.pop(client_id, None)
            self._partial_sent_count.pop(client_id, None)

    def _flow_keys(self):
        keys = super()._flow_keys()
        return keys | set(self._partial_processed_count) | set(self._processed_counts) | set(self._sent_count) | set(self._partial_sent_count)

    def _snapshot_flow(self, client_id):
        record = super()._snapshot_flow(client_id)
        record["processed"] = self._processed_counts.get(client_id, 0)
        record["partial_processed"] = self._partial_processed_count.get(client_id, 0)
        record["sent"] = self._sent_count.get(client_id, 0)
        record["partial_sent"] = self._partial_sent_count.get(client_id, 0)
        return record

    def _restore_snapshot(self, record):
        super()._restore_snapshot(record)
        key = record["c"]
        self._processed_counts[key] = record["processed"]
        self._partial_processed_count[key] = record["partial_processed"]
        self._sent_count[key] = record["sent"]
        self._partial_sent_count[key] = record["partial_sent"]

    def _get_next_node_id(self):
        return (self._node_id + 1) % self._ring_size

    def _handle_eof_message(self, client_id, eof, output_exchange=None):
        """
        Handle an EOF message by sending a RING_EOF to the next node in the ring.
        """
        if output_exchange is None:
            output_exchange = self._output_control_middleware

        total_processed_count = self._get_total_count(
            client_id,
            self._processed_counts,
            self._partial_processed_count,
            self._processed_counts_lock,
            0,
        )
        cleanup = eof.message_count == CLEANUP_EXPECTED_COUNT
        start_phase = RING_PHASE_CLEANUP if cleanup else RING_PHASE_COUNT
        output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.RING_EOF,
                client_id,
                RingEOF(
                    expected_count=eof.message_count,
                    total_processed_count=total_processed_count,
                    total_sent_count=self._get_total_sent_count(client_id, 0),
                ),
                message_id=ring_id(client_id, start_phase, 0),
            ),
            routing_key=self._get_ring_routing_key(self._get_next_node_id()),
        )

    def _handle_data_message(self, msg_type, client_id, payload):
        self._increment_processed_count(client_id)

    def _replay_record(self, record):
        super()._replay_record(record)
        if record["ch"] == MAIN_CHANNEL and not record.get("eof"):
            self._increment_processed_count(record["c"])
        elif record["ch"] == CONTROL_CHANNEL:
            self._restore_control_state(record["c"], record["ring"])

    def _flush_sharded(
        self,
        out_middleware,
        msg_type,
        client_id,
        items,
        key_of,
        num_shards,
        batch_size,
        routing_key_for=str,
    ):
        """
        Deterministic flush: shard `items` by md5(key_of(item)) and, within each
        shard, split into ceil(n/batch_size) buckets from the same hash, so the
        batching depends only on content (reproducible on replay). Each batch is
        sent with routing key routing_key_for(shard) and a flush_id keyed by bucket.
        """
        by_shard = defaultdict(list)
        for item in items:
            h = hash_of(key_of(item))
            by_shard[h % num_shards].append((h, item))
        for shard, pairs in by_shard.items():
            num_buckets = max(1, math.ceil(len(pairs) / batch_size))
            buckets = defaultdict(list)
            for h, item in pairs:
                buckets[(h // num_shards) % num_buckets].append(item)
            for bucket, batch in buckets.items():
                self._send(
                    out_middleware,
                    msg_type,
                    client_id,
                    batch,
                    routing_key=routing_key_for(shard),
                    message_id=flush_id(self._node_id, client_id, bucket),
                )
                self._increment_sent_count(client_id)

    def start(self):
        self._recover()
        self._control_thread = threading.Thread(
            target=self._input_control_middleware.start_consuming,
            args=(self._handle_control_message,),
            daemon=True,
        )
        self._control_thread.start()
        super().start()

    def shutdown(self):
        super().shutdown()
        if self._control_thread and self._control_thread.is_alive():
            self._input_control_middleware.stop_consuming_threadsafe()
            self._control_thread.join()
        self._input_control_exchange.close()
        self._output_control_exchange.close()
        self._control_output_control_exchange.close()
