import json
import logging
import os
import threading
from dataclasses import asdict
from datetime import datetime

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareExchangeFanoutRabbitMQ,
    MessageMiddlewareExchangeTopicRabbitMQ,
)
from common.models.query_results import Q3Result
from common.models.transaction import Transaction
from common.protocol import internal
from common.worker.sent_coordinated_worker import SentCoordinatedWorker
from config import Config

QUERY_ID = 3
DRAIN_BATCH_SIZE = 1000


class AnomalyFilter(SentCoordinatedWorker):
    def __init__(self, config):
        super().__init__()
        self.config = config

        self._avgs = {}
        self._avgs_ready = {}
        self._avg_batch_counts = {}
        self._avg_expected_counts = {}
        self._flow_locks = {}
        self._flow_locks_guard = threading.Lock()
        self._output_lock = threading.Lock()
        self._spill_files = {}

        self._avg_thread = None

        os.makedirs(self.config.spill_dir, exist_ok=True)

        self._input_topic = MessageMiddlewareExchangeTopicRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            binding_patterns=config.input_routing_keys,
            queue_name=config.input_queue_name,
        )
        self._input_avg_exchange = MessageMiddlewareExchangeFanoutRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.avg_exchange,
        )
        self._output_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
            routing_keys=[],
        )
        self._input_control_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.control_exchange,
            routing_keys=[self._ring_routing_key(config.node_id)],
        )
        self._output_control_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.control_exchange,
            routing_keys=[],
        )
        self._control_output_control_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.control_exchange,
            routing_keys=[],
        )

    @property
    def _node_id(self):
        return self.config.node_id

    @property
    def _ring_size(self):
        return self.config.ring_size

    @property
    def _input_middleware(self):
        return self._input_topic

    @property
    def _output_middleware(self):
        return self._output_exchange

    @property
    def _input_control_middleware(self):
        return self._input_control_exchange

    @property
    def _output_control_middleware(self):
        return self._output_control_exchange

    @property
    def _control_output_control_middleware(self):
        return self._control_output_control_exchange

    def _ring_routing_key(self, node_id):
        return f"{self.config.node_prefix}{node_id}"

    def _flow_key(self, client_id, gateway_id):
        return (client_id, gateway_id)

    def _get_flow_lock(self, key):
        with self._flow_locks_guard:
            lock = self._flow_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._flow_locks[key] = lock
            return lock

    def _get_avgs_ready_event(self, key):
        with self._flow_locks_guard:
            event = self._avgs_ready.get(key)
            if event is None:
                event = threading.Event()
                self._avgs_ready[key] = event
            return event

    def _spill_path(self, key):
        client_id, gateway_id = key
        return os.path.join(
            self.config.spill_dir, f"{client_id}__{gateway_id}.jsonl"
        )

    def _spill_file(self, key):
        f = self._spill_files.get(key)
        if f is None:
            f = open(self._spill_path(key), "a", encoding="utf-8")
            self._spill_files[key] = f
        return f

    def _close_spill(self, key):
        f = self._spill_files.pop(key, None)
        if f is not None:
            try:
                f.close()
            except Exception:
                pass

    def _serialize_tx(self, tx):
        d = asdict(tx)
        d["timestamp"] = tx.timestamp.isoformat()
        return json.dumps(d)

    def _deserialize_tx(self, line):
        d = json.loads(line)
        d["timestamp"] = datetime.fromisoformat(d["timestamp"])
        return Transaction(**d)

    def _write_spill(self, key, batch):
        f = self._spill_file(key)
        for tx in batch:
            f.write(self._serialize_tx(tx))
            f.write("\n")
        f.flush()
        os.fsync(f.fileno())

    def _is_anomalous(self, tx, avgs):
        avg = avgs.get(tx.payment_format.strip().lower())
        if avg is None or avg <= 0:
            return False
        return tx.amount < self.config.anomaly_threshold * avg

    def _payment_format_key(self, payment_format):
        return payment_format.strip().lower()

    def _filter_and_emit(self, client_id, gateway_id, transactions, avgs):
        result_batch = []
        for tx in transactions:
            if self._is_anomalous(tx, avgs):
                result_batch.append(
                    Q3Result(
                        from_bank=str(tx.from_bank),
                        from_account=tx.from_account,
                        amount_paid=tx.amount,
                    )
                )
        logging.info(
            "filter_and_emit: in=%d anomalous=%d avgs_keys=%s",
            len(transactions),
            len(result_batch),
            list(avgs.keys()) if avgs else [],
        )
        if not result_batch:
            return
        with self._output_lock:
            self._output_exchange.send(
                internal.serialize_msg(
                    internal.MsgType.Q3_RESULT_BATCH,
                    client_id,
                    gateway_id,
                    result_batch,
                ),
                routing_key=gateway_id,
            )
        self._increment_sent_count(client_id, gateway_id)

    def _drain_spill(self, client_id, gateway_id, avgs):
        key = self._flow_key(client_id, gateway_id)
        self._close_spill(key)
        path = self._spill_path(key)
        if not os.path.exists(path):
            return
        batch = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                batch.append(self._deserialize_tx(line))
                if len(batch) >= DRAIN_BATCH_SIZE:
                    self._filter_and_emit(client_id, gateway_id, batch, avgs)
                    batch = []
        if batch:
            self._filter_and_emit(client_id, gateway_id, batch, avgs)
        os.remove(path)

    def _handle_data_message(self, _, client_id, gateway_id, batch):
        key = self._flow_key(client_id, gateway_id)
        event = self._get_avgs_ready_event(key)
        avgs = None
        with self._get_flow_lock(key):
            super()._handle_data_message(_, client_id, gateway_id, batch)
            if event.is_set():
                avgs = self._avgs.get(key, {})
            else:
                self._write_spill(key, batch)
                return
        self._filter_and_emit(client_id, gateway_id, batch, avgs)

    def _handle_avg_message(self, message, ack, nack):
        try:
            msg_type, client_id, gateway_id, payload = internal.deserialize_msg(message)
            if msg_type == internal.MsgType.PAYMENT_FORMAT_AVERAGE_BATCH:
                self._merge_avgs(client_id, gateway_id, payload)
                ack()
                return
            if msg_type == internal.MsgType.EOF:
                self._on_avg_eof(client_id, gateway_id, payload)
                ack()
                return
            logging.warning("Unexpected avg message type: %s", msg_type)
            nack()
        except Exception as e:
            logging.error("Error handling avg message: %s", e)
            nack()
            raise

    def _merge_avgs(self, client_id, gateway_id, batch):
        key = self._flow_key(client_id, gateway_id)
        with self._get_flow_lock(key):
            table = self._avgs.setdefault(key, {})
            for entry in batch:
                table[self._payment_format_key(entry.payment_format)] = (
                    entry.average_amount
                )
            self._avg_batch_counts[key] = self._avg_batch_counts.get(key, 0) + 1
            self._mark_avgs_ready_if_complete(client_id, gateway_id)

    def _on_avg_eof(self, client_id, gateway_id, eof):
        key = self._flow_key(client_id, gateway_id)
        with self._get_flow_lock(key):
            self._avg_expected_counts[key] = eof.message_count
            logging.info(
                "AVG EOF for %s: received=%s expected=%s avgs=%s spill_exists=%s",
                key,
                self._avg_batch_counts.get(key, 0),
                eof.message_count,
                self._avgs.get(key, {}),
                os.path.exists(self._spill_path(key)),
            )
            self._mark_avgs_ready_if_complete(client_id, gateway_id)

    def _mark_avgs_ready_if_complete(self, client_id, gateway_id):
        key = self._flow_key(client_id, gateway_id)
        event = self._get_avgs_ready_event(key)
        if event.is_set():
            return

        expected_count = self._avg_expected_counts.get(key)
        if expected_count is None:
            return

        received_count = self._avg_batch_counts.get(key, 0)
        if received_count < expected_count:
            return

        avgs = self._avgs.get(key, {})
        logging.info(
            "AVG table ready for %s: received=%s expected=%s avgs=%s spill_exists=%s",
            key,
            received_count,
            expected_count,
            avgs,
            os.path.exists(self._spill_path(key)),
        )
        self._drain_spill(client_id, gateway_id, avgs)
        event.set()

    def _drop_flow_state(self, key):
        self._avgs.pop(key, None)
        self._avgs_ready.pop(key, None)
        self._avg_batch_counts.pop(key, None)
        self._avg_expected_counts.pop(key, None)
        self._flow_locks.pop(key, None)

    def _flush_data(self, client_id, gateway_id):
        key = self._flow_key(client_id, gateway_id)
        self._get_avgs_ready_event(key).wait()
        with self._get_flow_lock(key):
            self._drop_flow_state(key)
            self._close_spill(key)
            path = self._spill_path(key)
            if os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass

    def _send_final_eof(self, client_id, gateway_id, eof):
        with self._output_lock:
            self._output_exchange.send(
                internal.serialize_msg(
                    internal.MsgType.QUERY_END,
                    client_id,
                    gateway_id,
                    QUERY_ID,
                    eof.message_count,
                ),
                routing_key=gateway_id,
            )

    def start(self):
        self._avg_thread = threading.Thread(
            target=self._input_avg_exchange.start_consuming,
            args=(self._handle_avg_message,),
            daemon=True,
        )
        self._avg_thread.start()
        super().start()

    def shutdown(self):
        super().shutdown()
        if self._avg_thread and self._avg_thread.is_alive():
            self._input_avg_exchange.stop_consuming_threadsafe()
            self._avg_thread.join()
        self._input_avg_exchange.close()
        for key in list(self._spill_files.keys()):
            self._close_spill(key)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [AnomalyFilter] %(levelname)s %(message)s",
    )
    config = Config()
    worker = AnomalyFilter(config)
    try:
        worker.start()
    except Exception as e:
        logging.error(f"Error during AnomalyFilter execution: {e}")
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
