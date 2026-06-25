import logging

from common.communication.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)

from common.idempotency.ids import eof_id, final_eof_id
from common.models.bank_max_partial import BankMaxPartial
from common.communication.protocol import internal
from common.worker.stateful_worker import StatefulWorker
from config import Config


class BankMaxReducer(StatefulWorker):
    def __init__(self, config):
        super().__init__(config)
        self._global_max = {}  # (client_id) -> {from_bank: BankMaxPartial}

        self._input_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            routing_keys=[config.shard_id],
            queue_name=f"{config.input_exchange}_{config.shard_id}",
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






    @staticmethod
    def _beats(amount, account, cur_amount, cur_account):
        return amount > cur_amount or (amount == cur_amount and account < cur_account)

    def _handle_data_message(self, _, client_id, bank_max_batch):
        super()._handle_data_message(_, client_id, bank_max_batch)
        delta = {}
        for bank_max in bank_max_batch:
            current = delta.get(bank_max.from_bank)
            if current is None or self._beats(
                bank_max.amount, bank_max.from_account, current[1], current[0]
            ):
                delta[bank_max.from_bank] = [bank_max.from_account, bank_max.amount]
        self._apply_delta(client_id, delta)
        return delta

    def _cleanup_state(self, client_id):
        super()._cleanup_state(client_id)
        self._global_max.pop(client_id, None)

    def _apply_delta(self, client_id, delta):
        flow_max = self._global_max.setdefault(client_id, {})
        for from_bank, (from_account, amount) in delta.items():
            current = flow_max.get(from_bank)
            if current is None or self._beats(
                amount, from_account, current.amount, current.from_account
            ):
                flow_max[from_bank] = BankMaxPartial(
                    from_bank=from_bank,
                    from_account=from_account,
                    amount=amount,
                )

    def _state_as_delta(self, client_id):
        return {
            from_bank: [bank_max.from_account, bank_max.amount]
            for from_bank, bank_max in self._global_max.get(
                (client_id), {}
            ).items()
        }

    def _flush_data(self, client_id):
        flow_max = self._global_max.pop(client_id, {})
        self._flush_sharded(
            self._output_exchange,
            internal.MsgType.BANK_MAX_PARTIAL_BATCH,
            client_id,
            list(flow_max.values()),
            key_of=lambda bank_max: bank_max.from_bank,
            num_shards=self.config.output_node_count,
            batch_size=self.config.batch_size,
        )

    def _send_final_eof(self, client_id, eof):
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.EOF,
                client_id,
                eof,
                message_id=final_eof_id(client_id, eof),
            ),
            routing_key="0",
        )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [BankMaxReducer] %(levelname)s %(message)s",
    )
    config = Config()
    reducer = BankMaxReducer(config)
    try:
        reducer.start()
    except Exception as e:
        logging.error(f"Error during BankMaxReducer execution: {e}")
    finally:
        reducer.shutdown()


if __name__ == "__main__":
    main()
