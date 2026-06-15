import logging

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeTopicRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.models.transaction_for_currency_conversion import (
    TransactionForCurrencyConversion,
)
from common.ids import eof_id
from common.protocol.internal import internal
from common.worker.stateless_worker import StatelessWorker
from config import Config


class PaymentFormatFilter(StatelessWorker):
    def __init__(self, config):
        super().__init__(config)

        self._input_exchange = MessageMiddlewareExchangeTopicRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            binding_patterns=config.input_routing_keys,
            queue_name=config.input_queue_name,
        )
        self._output_queue = MessageMiddlewareQueueRabbitMQ(
            host=config.rabbitmq_host,
            queue_name=config.output_queue,
        )

    @property
    def _input_middleware(self):
        return self._input_exchange

    @property
    def _output_middleware(self):
        return self._output_queue

    def _send_final_eof(self, client_id, gateway_id, eof):
        self._output_queue.send(
            internal.serialize_msg(
                internal.MsgType.EOF,
                client_id,
                gateway_id,
                eof,
                message_id=eof_id(client_id, gateway_id),
            )
        )

    def _handle_data_message(self, _, client_id, gateway_id, payload):
        """
        Handle incoming data messages by filtering transactions based on their payment format and sending the valid transactions to the output queue.
        """
        filtered = [
            TransactionForCurrencyConversion(
                timestamp=transaction.timestamp.strftime("%Y/%m/%d %H:%M"),
                amount=transaction.amount,
                currency=transaction.currency,
            )
            for transaction in payload
            if None
            not in (
                transaction.timestamp,
                transaction.payment_format,
                transaction.amount,
                transaction.currency,
            )
            and transaction.payment_format.lower() in self.config.valid_payment_formats
        ]
        self._send(
            self._output_queue,
            internal.MsgType.CURRENCY_CONVERSION_BATCH,
            client_id,
            gateway_id,
            filtered,
        )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [PaymentFormatFilter] %(levelname)s %(message)s",
    )
    config = Config()
    payment_format_filter = PaymentFormatFilter(config)
    try:
        payment_format_filter.start()
    except Exception as e:
        logging.error(f"Error during PaymentFormatFilter execution: {e}")
    finally:
        payment_format_filter.shutdown()


if __name__ == "__main__":
    main()
