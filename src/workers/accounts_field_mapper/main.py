import logging

from common.models.bank import Bank
from common.protocol import internal
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareExchangeFanoutRabbitMQ,
)
from common.worker.stateless_worker import StatelessWorker
from config import Config


class AccountsFieldMapper(StatelessWorker):
    def __init__(self, config):
        """
        Initialize the AccountsFieldMapper worker with the given configuration.
        """
        super().__init__()
        self.config = config

        self._input_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.raw_data_exchange,
            routing_keys=[config.input_routing_key],
        )
        self._output_exchange = MessageMiddlewareExchangeFanoutRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
        )

    @property
    def _input_middleware(self):
        """
        Return the input exchange to consume raw accounts from the gateway.
        """
        return self._input_exchange

    @property
    def _output_middleware(self):
        """
        Return the output fanout exchange to broadcast filtered banks downstream.
        """
        return self._output_exchange

    def _handle_data_message(self, _, client_id, gateway_id, payload):
        """
        Parse each raw CSV line into a Bank and broadcast the batch to the output exchange.
        """
        banks = [self._parse(raw_acc.raw) for raw_acc in payload]
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.BANK_BATCH, client_id, gateway_id, banks
            )
        )

    def _parse(self, raw):
        """
        Parse a CSV line into a Bank dataclass.
        """
        fields = raw.split(",")
        return Bank(bank_id=fields[1], name=fields[0])


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [AccountsFieldMapper] %(levelname)s %(message)s",
    )
    config = Config.from_env()
    worker = AccountsFieldMapper(config)
    try:
        worker.start()
    except Exception as e:
        logging.error(f"Error during AccountsFieldMapper execution: {e}")
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
