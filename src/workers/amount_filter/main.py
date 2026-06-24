import logging

from common.communication.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareExchangeTopicRabbitMQ,
)
from common.idempotency.ids import eof_id, final_eof_id
from common.models.query_results import Q1Result
from common.communication.protocol import internal
from common.worker.stateless_worker import StatelessWorker
from config import Config


class AmountFilter(StatelessWorker):
    def __init__(self, config):
        super().__init__(config)

        self._input_exchange = MessageMiddlewareExchangeTopicRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            binding_patterns=[
                config.input_routing_key,
                config.input_eof_routing_key,
            ],
            queue_name=config.input_queue_name,
        )
        self._output_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
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

    def _has_required_fields(self, data_record):
        required_fields = [
            "from_bank",
            "from_account",
            "to_bank",
            "to_account",
            "amount",
        ]
        return all(getattr(data_record, field) is not None for field in required_fields)

    def _handle_data_message(self, _, client_id, payload):
        filtered = []
        for tx in payload:
            if not self._has_required_fields(tx):
                continue
            if tx.amount < self.config.amount_threshold:
                filtered.append(
                    Q1Result(
                        from_bank=tx.from_bank,
                        from_account=tx.from_account,
                        to_bank=tx.to_bank,
                        to_account=tx.to_account,
                        amount_paid=tx.amount,
                    )
                )
        self._send(
            self._output_exchange,
            internal.MsgType.Q1_RESULT_BATCH,
            client_id,
            filtered,
            routing_key=client_id,
        )

    def _send_final_eof(self, client_id, eof):
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.QUERY_END,
                client_id,
                self.config.query_id,
                eof.message_count,
                message_id=final_eof_id(client_id, eof, self.config.query_id),
            ),
            routing_key=client_id,
        )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [AmountFilter] %(levelname)s %(message)s",
    )
    config = Config()
    worker = AmountFilter(config)
    try:
        worker.start()
    except Exception as e:
        logging.error("Error during AmountFilter execution: %s", e)
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
