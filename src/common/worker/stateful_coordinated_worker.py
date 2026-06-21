import math
import threading
from collections import defaultdict

from common.ids import flush_id
from common.sharding import hash_of
from common.worker.ring_coordinated_worker import RingCoordinatedWorker


class StatefulCoordinatedWorker(RingCoordinatedWorker):
    def __init__(self, config):
        super().__init__(config)
        self._sent_count = (
            {}
        )  # client_id -> sent_count | actual total sent count
        self._partial_sent_count = (
            {}
        )  # client_id -> partial_sent_count | previous sent count sent to ring
        self._sent_count_lock = threading.Lock()

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
            self._sent_count[client_id] = (
                self._sent_count.get(client_id, 0) + 1
            )

    def _control_state_snapshot(self, client_id):
        snapshot = super()._control_state_snapshot(client_id)
        snapshot["sent_count"] = self._sent_count.get(client_id, 0)
        snapshot["partial_sent"] = self._partial_sent_count.get(client_id, 0)
        return snapshot

    def _restore_control_state(self, client_id, snapshot):
        super()._restore_control_state(client_id, snapshot)
        self._sent_count[client_id] = snapshot["sent_count"]
        self._partial_sent_count[client_id] = snapshot["partial_sent"]

    def _flow_keys(self):
        keys = super()._flow_keys()
        return keys | set(self._sent_count) | set(self._partial_sent_count)

    def _snapshot_flow(self, client_id):
        record = super()._snapshot_flow(client_id)
        record["sent"] = self._sent_count.get(client_id, 0)
        record["partial_sent"] = self._partial_sent_count.get(client_id, 0)
        return record

    def _restore_snapshot(self, record):
        super()._restore_snapshot(record)
        key = record["c"]
        self._sent_count[key] = record["sent"]
        self._partial_sent_count[key] = record["partial_sent"]

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
