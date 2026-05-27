import json
import logging
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
from common.utils import BatchSpill
from common.worker.side_input_stateless_coordinated_worker import (
    SideInputStatelessCoordinatedWorker,
)
from common.worker.safe_output_capable import SafeOutputCapable
from config import Config


class AnomalyFilter(SafeOutputCapable, SideInputStatelessCoordinatedWorker):
    def __init__(self, config):
        self.config = config
        super().__init__()

        self._avgs = {}
        self._flow_locks = {}
        self._flow_locks_guard = threading.Lock()

        self._spill = BatchSpill(
            spill_dir=self.config.spill_dir,
            serialize=lambda batch: json.dumps(
                [self._serialize_tx(tx) for tx in batch]
            ),
            deserialize=lambda line: [
                self._deserialize_tx(d) for d in json.loads(line)
            ],
        )

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
        self._control_output_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
            routing_keys=[],
        )

    @property
    def _input_middleware(self):
        return self._input_topic

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

    def _is_anomalous(self, tx, avgs):
        avg = avgs.get(tx.payment_format.strip().lower())
        if avg is None or avg <= 0:
            return False
        return tx.amount < self.config.anomaly_threshold * avg

    def _payment_format_key(self, payment_format):
        return payment_format.strip().lower()

    def _filter_and_emit(self, client_id, gateway_id, transactions, avgs, exchange):
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
        exchange.send(
            internal.serialize_msg(
                internal.MsgType.Q3_RESULT_BATCH,
                client_id,
                gateway_id,
                result_batch,
            ),
            routing_key=gateway_id,
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
                self._spill.write(key, batch)
                return
        self._filter_and_emit(client_id, gateway_id, batch, avgs, self._output_exchange)

    def _process_side_batch(self, client_id, gateway_id, batch):
        key = self._flow_key(client_id, gateway_id)
        with self._get_flow_lock(key):
            table = self._avgs.setdefault(key, {})
            for entry in batch:
                table[self._payment_format_key(entry.payment_format)] = (
                    entry.average_amount
                )

    def _drop_flow_state(self, key):
        self._avgs.pop(key, None)
        self._side_input.drop(key)
        self._flow_locks.pop(key, None)

    def _flush_data(self, client_id, gateway_id):
        key = self._flow_key(client_id, gateway_id)
        avgs = self._avgs.get(key, {})
        self._spill.drain(
            key,
            lambda batch: self._filter_and_emit(
                client_id, gateway_id, batch, avgs, self._control_output_exchange
            ),
        )
        with self._get_flow_lock(key):
            self._drop_flow_state(key)

    def _send_final_eof(self, client_id, gateway_id, eof):
        self._control_output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.QUERY_END,
                client_id,
                gateway_id,
                self.config.query_id,
                eof.message_count,
            ),
            routing_key=gateway_id,
        )

    def shutdown(self):
        super().shutdown()
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
