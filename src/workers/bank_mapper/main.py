import logging
import threading

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareExchangeFanoutRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.models.query_results import Q2Result
from common.protocol import internal
from common.worker.stateless_coordinated_worker import StatelessCoordinatedWorker
from config import Config

QUERY_ID = 2


class BankMapper(StatelessCoordinatedWorker):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self._bank_names_lock = threading.Lock()
        self._banks_loaded = {}
        self._banks_thread = None
        self._bank_names = {}

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

    def _ring_routing_key(self, node_id):
        return f"{self.config.node_prefix}{node_id}"

    def _flow_key(self, client_id, gateway_id):
        return (client_id, gateway_id)

    def _get_banks_loaded_event(self, client_id, gateway_id):
        key = self._flow_key(client_id, gateway_id)
        with self._bank_names_lock:
            if key not in self._banks_loaded:
                self._banks_loaded[key] = threading.Event()
            return self._banks_loaded[key]

    def _store_bank(self, client_id, gateway_id, bank):
        key = self._flow_key(client_id, gateway_id)
        bank_id = str(int(bank.bank_id))
        with self._bank_names_lock:
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

    def _handle_bank_message(self, message, ack, nack):
        try:
            msg_type, client_id, gateway_id, payload = internal.deserialize_msg(message)
            if msg_type == internal.MsgType.BANK_BATCH:
                for bank in payload:
                    self._store_bank(client_id, gateway_id, bank)
                ack()
                return

            if msg_type == internal.MsgType.EOF:
                logging.info(
                    "Bank catalog EOF received for client %s/%s.",
                    client_id,
                    gateway_id,
                )
                self._get_banks_loaded_event(client_id, gateway_id).set()
                ack()
                return

            logging.warning("Unexpected bank catalog message type: %s", msg_type)
            nack()
        except Exception as e:
            logging.error("Error handling bank catalog message: %s", e)
            nack()
            raise

    def _handle_data_message(self, _, client_id, gateway_id, bank_max_batch):
        super()._handle_data_message(_, client_id, gateway_id, bank_max_batch)
        self._get_banks_loaded_event(client_id, gateway_id).wait()
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
                internal.MsgType.Q2_RESULT_BATCH, client_id, gateway_id, mapped_batch
            ),
            routing_key=gateway_id,
        )

    def _flush_data(self, client_id, gateway_id):
        key = self._flow_key(client_id, gateway_id)
        with self._bank_names_lock:
            self._bank_names.pop(key, None)
            self._banks_loaded.pop(key, None)

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

    def start(self):
        self._banks_thread = threading.Thread(
            target=self._input_banks_exchange.start_consuming,
            args=(self._handle_bank_message,),
            daemon=True,
        )
        self._banks_thread.start()
        super().start()

    def shutdown(self):
        super().shutdown()
        if self._banks_thread and self._banks_thread.is_alive():
            self._input_banks_exchange.stop_consuming_threadsafe()
            self._banks_thread.join()
        self._input_banks_exchange.close()


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
