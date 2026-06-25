import logging
from datetime import datetime

import requests

from common.communication.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.idempotency.ids import eof_id, final_eof_id
from common.models.transaction_amount import TransactionAmount
from common.communication.protocol import internal
from common.worker.utils.sharding import shard_of
from common.worker.stateless_worker import StatelessWorker
from config import Config

EOF_SHARD = "0"


class CurrencyMapper(StatelessWorker):
    _CURRENCY_NAME_TO_ISO = {
        "australian dollar": "AUD",
        "bitcoin": "BTC",
        "brazil real": "BRL",
        "canadian dollar": "CAD",
        "euro": "EUR",
        "mexican peso": "MXN",
        "ruble": "RUB",
        "rupee": "INR",
        "saudi riyal": "SAR",
        "shekel": "ILS",
        "swiss franc": "CHF",
        "uk pound": "GBP",
        "yen": "JPY",
        "yuan": "CNY",
    }
    # Bitcoin rates taken from investing.com
    _BTC_RATES = {
        "2022-09-01": 1.0 / 19793.1,
        "2022-09-02": 1.0 / 199999.0,
        "2022-09-03": 1.0 / 19831.4,
        "2022-09-04": 1.0 / 19952.7,
        "2022-09-05": 1.0 / 20126.1,
    }
    _DEFAULT_RATE = 1.0
    _DECIMAL_PLACES = 2

    def __init__(self, config: Config):
        super().__init__(config)
        self._rates = self._fetch_rates()

        self._input_queue = MessageMiddlewareQueueRabbitMQ(
            host=config.rabbitmq_host,
            queue_name=config.input_queue,
        )
        self._output_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.output_exchange,
            routing_keys=[],
        )

    @property
    def _input_middleware(self):
        return self._input_queue

    @property
    def _output_middleware(self):
        return self._output_exchange

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
        for date_str, btc_rate in self._BTC_RATES.items():
            rates.setdefault(date_str, {})["BTC"] = btc_rate
        return rates

    def _resolve_rate(self, amount, currency, timestamp):
        """
        Resolve the rate for a given transaction timestamp and currency by looking up the appropriate rate from the fetched rates, returning the rate if found or a default rate if not found.
        """
        iso_code = self._CURRENCY_NAME_TO_ISO.get(currency.lower())
        date_str = datetime.strptime(
            timestamp, self.config.transaction_date_format
        ).strftime(self.config.rates_date_format)
        rate = (
            self._rates.get(date_str, {}).get(iso_code, self._DEFAULT_RATE)
            if iso_code
            else self._DEFAULT_RATE
        )
        return round(float(amount) * (1.0 / rate), self._DECIMAL_PLACES)

    def _send_final_eof(self, client_id, eof):
        self._send(
            self._output_exchange,
            internal.MsgType.EOF,
            client_id,
            eof,
            routing_key=EOF_SHARD,
            message_id=final_eof_id(client_id, eof),
        )

    def _handle_data_message(self, _, client_id, payload):
        converted = [
            TransactionAmount(
                amount=self._resolve_rate(
                    transaction.amount, transaction.currency, transaction.timestamp
                )
            )
            for transaction in payload
        ]
        shard = shard_of(self._current_message_id, self.config.output_node_count)
        self._send(
            self._output_exchange,
            internal.MsgType.AMOUNT_TRANSACTION_BATCH,
            client_id,
            converted,
            routing_key=str(shard),
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
