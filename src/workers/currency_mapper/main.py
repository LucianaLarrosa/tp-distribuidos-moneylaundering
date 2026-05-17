import logging
from datetime import datetime

import requests

from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from common.models.transaction_amount import TransactionAmount
from common.protocol import internal
from common.worker.stateless_worker import StatelessWorker
from config import Config


class CurrencyMapper(StatelessWorker):
    _CURRENCY_NAME_TO_ISO = {
        "australian dollar": "AUD",
        "bitcoin": "BTC",
        "brazil real": "BRL",
        "canadian dollar": "CAD",
        "euro": "EUR",
        "mexico peso": "MXN",
        "ruble": "RUB",
        "rupee": "INR",
        "saudi riyal": "SAR",
        "shekel": "ILS",
        "swiss franc": "CHF",
        "uk pound": "GBP",
        "yen": "JPY",
        "yuan": "CNY",
    }
    _DEFAULT_RATE = 1.0
    _DECIMAL_PLACES = 2

    def __init__(self, config: Config):
        """
        Initialize the CurrencyMapper worker with the given configuration, fetching the necessary rates from the Frankfurter API.
        """
        super().__init__()
        self.config = config
        self._rates = self._fetch_rates()

        self._input_queue = MessageMiddlewareQueueRabbitMQ(
            host=config.rabbitmq_host,
            queue_name=config.input_queue,
        )
        self._output_queue = MessageMiddlewareQueueRabbitMQ(
            host=config.rabbitmq_host,
            queue_name=config.output_queue,
        )

    @property
    def _input_middleware(self):
        """
        Return the input queue to consume messages from the previous stage.
        """
        return self._input_queue

    @property
    def _output_middleware(self):
        """
        Return the output queue to forward messages to the next stage.
        """
        return self._output_queue

    def _fetch_rates(self):
        """
        Fetch the rates from the configured Frankfurter API endpoint, returning a dictionary of rates indexed by date and currency ISO code.
        """
        response = requests.get(
            self.config.frankfurter_url,
            timeout=self.config.frankfurter_timeout_seconds,
        )
        response.raise_for_status()
        rates = {}
        for entry in response.json():
            rates.setdefault(entry[self.config.rates_date_field], {})[
                entry[self.config.rates_quote_field]
            ] = entry[self.config.rates_rate_field]
        return rates

    def _resolve_rate(self, timestamp, currency):
        """
        Resolve the rate for a given transaction timestamp and currency by looking up the appropriate rate from the fetched rates, returning the rate if found or a default rate if not found.
        """
        iso_code = self._CURRENCY_NAME_TO_ISO.get(currency.lower())
        date_str = datetime.strptime(
            timestamp, self.config.transaction_date_format
        ).strftime(self.config.rates_date_format)
        if (
            iso_code is not None
            and date_str in self._rates
            and iso_code in self._rates[date_str]
        ):
            return self._rates[date_str][iso_code]
        return self._DEFAULT_RATE

    def _convert(self, transaction):
        """
        Convert the amount of a transaction to the target currency using the appropriate rate, returning a TransactionAmount with the converted amount.
        """
        rate = self._resolve_rate(transaction.timestamp, transaction.currency)
        return TransactionAmount(
            amount=round(transaction.amount * (1.0 / rate), self._DECIMAL_PLACES),
        )

    def _handle_data_message(self, _, client_id, gateway_id, payload):
        """
        Handle incoming data messages by converting transaction amounts to the target currency and sending the converted amounts to the output queue.
        """
        converted = [self._convert(transaction) for transaction in payload]
        self._output_queue.send(
            internal.serialize_msg(
                internal.MsgType.AMOUNT_TRANSACTION_BATCH,
                client_id,
                gateway_id,
                converted,
            )
        )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [CurrencyMapper] %(levelname)s %(message)s",
    )
    config = Config()
    currency_mapper = CurrencyMapper(config)
    try:
        currency_mapper.start()
    except Exception as e:
        logging.error("Error during CurrencyMapper execution: %s", e)
    finally:
        currency_mapper.shutdown()


if __name__ == "__main__":
    main()
