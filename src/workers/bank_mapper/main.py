import csv
import logging

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.models.query_results import Q2Result
from common.protocol import internal
from common.worker.stateless_coordinated_worker import StatelessCoordinatedWorker
from config import Config


class BankMapper(StatelessCoordinatedWorker):
    def __init__(self, config, bank_names=None):
        super().__init__()
        self.config = config
        self._bank_names = (
            bank_names if bank_names is not None else self._load_bank_names()
        )

        self._input_queue = MessageMiddlewareQueueRabbitMQ(
            host=config.rabbitmq_host,
            queue_name=config.input_queue,
        )
        self._output_queue = MessageMiddlewareQueueRabbitMQ(
            host=config.rabbitmq_host,
            queue_name=config.output_queue,
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
        return self._output_queue

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

    def _load_bank_names(self):
        """
        Placeholder for the future communication with the bank-name provider node.
        """
        bank_names = {}
        if self.config.accounts_path is None:
            return bank_names

        with open(self.config.accounts_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                bank_id = row["Bank ID"]
                bank_name = row["Bank Name"]
                bank_names[bank_id] = bank_name
                bank_names[str(int(bank_id))] = bank_name

        return bank_names

    def _handle_data_message(self, _, client_id, gateway_id, bank_max_batch):
        super()._handle_data_message(_, client_id, gateway_id, bank_max_batch)
        mapped_batch = []
        for bank_max in bank_max_batch:
            bank_id = str(int(bank_max.from_bank))
            mapped_batch.append(
                Q2Result(
                    bank_name=self._bank_names[bank_id],
                    from_account=bank_max.from_account,
                    amount_paid=bank_max.amount,
                )
            )

        self._output_queue.send(
            internal.serialize_msg(
                internal.MsgType.Q2_RESULT_BATCH, client_id, gateway_id, mapped_batch
            )
        )


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
