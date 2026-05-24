import json
import logging
import threading
from dataclasses import asdict

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareExchangeFanoutRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.models.bank_max_partial import BankMaxPartial
from common.models.query_results import Q2Result
from common.protocol import internal
from common.utils import BatchSpill
from common.worker.side_input_stateless_coordinated_worker import (
    SideInputStatelessCoordinatedWorker,
)
from config import Config

QUERY_ID = 2


class BankMapper(SideInputStatelessCoordinatedWorker):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self._bank_names_lock = threading.Lock()
        self._bank_names = {}
        self._flow_locks = {}
        self._flow_locks_guard = threading.Lock()

        self._spill = BatchSpill(
            spill_dir=self.config.spill_dir,
            serialize=lambda batch: json.dumps([asdict(bm) for bm in batch]),
            deserialize=lambda line: [BankMaxPartial(**d) for d in json.loads(line)],
        )

        self._input_queue = MessageMiddlewareQueueRabbitMQ(
            host=config.rabbitmq_host,
            queue_name=config.input_queue,
        )
        self._input_banks_exchange = MessageMiddlewareExchangeFanoutRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.banks_exchange,
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
        return self._input_queue

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

    @property
    def _input_side_middleware(self):
        return self._input_banks_exchange

    @property
    def _side_batch_msg_type(self):
        return internal.MsgType.BANK_BATCH

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

    def _store_bank_unlocked(self, key, bank, client_id, gateway_id):
        bank_id = str(int(bank.bank_id))
        bank_names = self._bank_names.setdefault(key, {})
        current = bank_names.get(bank_id)
        if current is None:
            bank_names[bank_id] = bank.name
            return
        if current != bank.name:
            logging.warning(
                "Bank id %s arrived with conflicting names for client %s/%s: %s / %s",
                bank_id,
                client_id,
                gateway_id,
                current,
                bank.name,
            )

    def _process_side_batch(self, client_id, gateway_id, batch):
        key = self._flow_key(client_id, gateway_id)
        with self._bank_names_lock:
            for bank in batch:
                self._store_bank_unlocked(key, bank, client_id, gateway_id)

    def _map_and_emit(self, client_id, gateway_id, bank_max_batch):
        key = self._flow_key(client_id, gateway_id)
        mapped_batch = []
        for bank_max in bank_max_batch:
            bank_id = str(int(bank_max.from_bank))
            with self._bank_names_lock:
                bank_name = self._bank_names[key][bank_id]
            mapped_batch.append(
                Q2Result(
                    bank_name=bank_name,
                    from_account=bank_max.from_account,
                    amount_paid=bank_max.amount,
                )
            )
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.Q2_RESULT_BATCH,
                client_id,
                gateway_id,
                mapped_batch,
            ),
            routing_key=gateway_id,
        )

    def _handle_data_message(self, _, client_id, gateway_id, bank_max_batch):
        key = self._flow_key(client_id, gateway_id)
        with self._get_flow_lock(key):
            super()._handle_data_message(_, client_id, gateway_id, bank_max_batch)
            if not self._side_input.is_ready(key):
                self._spill.write(key, bank_max_batch)
                return
        self._map_and_emit(client_id, gateway_id, bank_max_batch)

    def _flush_data(self, client_id, gateway_id):
        key = self._flow_key(client_id, gateway_id)
        self._spill.drain(
            key, lambda batch: self._map_and_emit(client_id, gateway_id, batch)
        )
        with self._bank_names_lock:
            self._bank_names.pop(key, None)
        self._side_input.drop(key)
        with self._flow_locks_guard:
            self._flow_locks.pop(key, None)

    def _send_final_eof(self, client_id, gateway_id, eof):
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

    def shutdown(self):
        super().shutdown()
        self._spill.close_all()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [BankMapper] %(levelname)s %(message)s",
    )
    config = Config()
    mapper = BankMapper(config)
    try:
        mapper.start()
    except Exception as e:
        logging.error(f"Error during BankMapper execution: {e}")
    finally:
        mapper.shutdown()


if __name__ == "__main__":
    main()
