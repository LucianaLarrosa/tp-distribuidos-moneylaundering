import json
import logging
import threading
from dataclasses import asdict
from datetime import datetime

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareExchangeFanoutRabbitMQ,
)
from common.ids import eof_id
from common.models.query_results import Q3Result
from common.models.transaction import Transaction
from common.protocol.internal import internal
from common.utils import BatchSpill
from common.worker.side_input_stateless_coordinated_worker import (
    SideInputStatelessCoordinatedWorker,
)
from common.worker.safe_output_capable import SafeOutputCapable
from config import Config


class AnomalyFilter(SafeOutputCapable, SideInputStatelessCoordinatedWorker):
    def __init__(self, config):
        super().__init__(config)

        self._avgs = {}
        self._flow_locks = {}
        self._flow_locks_guard = threading.Lock()

        self._spill = BatchSpill(
            spill_dir=self.config.spill_dir,
            serialize=lambda entry: json.dumps(
                {"mid": entry[0], "items": [self._serialize_tx(tx) for tx in entry[1]]}
            ),
            deserialize=lambda line: self._deserialize_entry(line),
        )

        self._input_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            routing_keys=[str(config.node_id)],
            queue_name=config.input_queue,
        )
        self._input_avg_exchange = MessageMiddlewareExchangeFanoutRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.avg_exchange,
            queue_name=f"{config.avg_exchange}_{config.node_id}",
        )
        self._output_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
            routing_keys=[],
        )
        self._control_output_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
            routing_keys=[],
        )
        self._side_output_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
            routing_keys=[],
        )

    @property
    def _input_middleware(self):
        return self._input_exchange

    @property
    def _output_middleware(self):
        return self._output_exchange

    @property
    def _rabbitmq_host(self):
        return self.config.rabbitmq_host

    @property
    def _control_exchange_name(self):
        return self.config.control_exchange

    @property
    def _node_prefix(self):
        return self.config.node_prefix

    @property
    def _node_id(self):
        return self.config.node_id

    @property
    def _ring_size(self):
        return self.config.ring_size

    @property
    def _input_side_middleware(self):
        return self._input_avg_exchange

    @property
    def _side_batch_msg_type(self):
        return internal.MsgType.PAYMENT_FORMAT_AVERAGE_BATCH

    @property
    def _control_output_middleware(self):
        return self._control_output_exchange

    def _flow_key(self, client_id, gateway_id):
        return (client_id, gateway_id)

    def _get_flow_lock(self, key):
        with self._flow_locks_guard:
            lock = self._flow_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._flow_locks[key] = lock
            return lock

    def _serialize_tx(self, tx):
        d = asdict(tx)
        d["timestamp"] = tx.timestamp.isoformat()
        return d

    def _deserialize_tx(self, d):
        d["timestamp"] = datetime.fromisoformat(d["timestamp"])
        return Transaction(**d)

    def _deserialize_entry(self, line):
        obj = json.loads(line)
        return (obj["mid"], [self._deserialize_tx(d) for d in obj["items"]])

    def _is_anomalous(self, tx, avgs):
        avg = avgs.get(tx.payment_format.strip().lower())
        if avg is None or avg <= 0:
            return False
        return tx.amount < self.config.anomaly_threshold * avg

    def _payment_format_key(self, payment_format):
        return payment_format.strip().lower()

    def _filter_and_emit(
        self, client_id, gateway_id, transactions, avgs, exchange, message_id
    ):
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
        self._send(
            exchange,
            internal.MsgType.Q3_RESULT_BATCH,
            client_id,
            gateway_id,
            result_batch,
            routing_key=client_id,
            message_id=message_id,
        )

    def _has_required_anomaly_fields(self, tx):
        return (
            tx.payment_format is not None
            and tx.amount is not None
            and tx.from_bank is not None
            and tx.from_account is not None
        )

    def _handle_data_message(self, _, client_id, gateway_id, batch):
        batch = [tx for tx in batch if self._has_required_anomaly_fields(tx)]
        key = self._flow_key(client_id, gateway_id)
        avgs = None
        with self._get_flow_lock(key):
            super()._handle_data_message(_, client_id, gateway_id, batch)
            if self._side_input.is_ready(key):
                avgs = self._avgs.get(key, {})
            else:
                self._spill.write(key, (self._current_message_id, batch))
                return
        self._filter_and_emit(
            client_id,
            gateway_id,
            batch,
            avgs,
            self._output_exchange,
            message_id=self._current_message_id,
        )

    def _side_delta(self, payload):
        return [
            [self._payment_format_key(entry.payment_format), entry.average_amount]
            for entry in payload
        ]

    def _apply_side_delta(self, client_id, gateway_id, delta):
        key = self._flow_key(client_id, gateway_id)
        with self._get_flow_lock(key):
            table = self._avgs.setdefault(key, {})
            for payment_format, avg in delta:
                table[payment_format] = avg

    def _on_side_input_ready(self, client_id, gateway_id):
        key = self._flow_key(client_id, gateway_id)
        with self._get_flow_lock(key):
            avgs = self._avgs.get(key, {})
            self._spill.drain(
                key,
                lambda entry: self._filter_and_emit(
                    client_id,
                    gateway_id,
                    entry[1],
                    avgs,
                    self._side_output_exchange,
                    message_id=entry[0],
                ),
            )

    def _drop_flow_state(self, key):
        self._avgs.pop(key, None)
        self._side_input.drop(key)
        with self._flow_locks_guard:
            self._flow_locks.pop(key, None)

    def _flush_data(self, client_id, gateway_id):
        key = self._flow_key(client_id, gateway_id)
        with self._get_flow_lock(key):
            avgs = self._avgs.get(key, {})
            self._spill.drain(
                key,
                lambda entry: self._filter_and_emit(
                    client_id,
                    gateway_id,
                    entry[1],
                    avgs,
                    self._control_output_exchange,
                    message_id=entry[0],
                ),
            )
            self._drop_flow_state(key)

    def _send_final_eof(self, client_id, gateway_id, eof):
        self._control_output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.QUERY_END,
                client_id,
                gateway_id,
                self.config.query_id,
                eof.message_count,
                message_id=eof_id(client_id, gateway_id, self.config.query_id),
            ),
            routing_key=client_id,
        )

    def shutdown(self):
        super().shutdown()
        self._side_output_exchange.close()
        self._control_output_exchange.close()
        self._spill.close_all()


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
