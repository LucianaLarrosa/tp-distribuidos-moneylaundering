import logging

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.ids import eof_id, final_eof_id
from common.models.bank_max_partial import BankMaxPartial
from common.protocol.internal import internal
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from config import Config


class BankMaxAggregator(StatefulCoordinatedWorker):
    def __init__(self, config):
        super().__init__(config)
        self._local_max = (
            {}
        )  # (client_id, gateway_id) -> {from_bank: tx_with_max_amount}

        self._input_queue = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            routing_keys=[str(config.node_id)],
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

    def _shard_routing_key(self, shard_id):
        return str(shard_id)

    def __has_required_anomaly_fields(self, tx):
        return (
            tx.amount is not None
            and tx.from_bank is not None
            and tx.from_account is not None
        )

    @staticmethod
    def _beats(amount, account, cur_amount, cur_account):
        return amount > cur_amount or (amount == cur_amount and account < cur_account)

    def _handle_data_message(self, _, client_id, gateway_id, transaction_batch):
        super()._handle_data_message(_, client_id, gateway_id, transaction_batch)
        delta = {}
        for transaction in transaction_batch:
            if not self.__has_required_anomaly_fields(transaction):
                continue
            from_bank = str(int(transaction.from_bank))
            current = delta.get(from_bank)
            if current is None or self._beats(
                transaction.amount, transaction.from_account, current[1], current[0]
            ):
                delta[from_bank] = [transaction.from_account, transaction.amount]
        self._apply_delta(client_id, gateway_id, delta)
        return delta

    def _cleanup_state(self, client_id, gateway_id):
        super()._cleanup_state(client_id, gateway_id)
        self._local_max.pop((client_id, gateway_id), None)

    def _apply_delta(self, client_id, gateway_id, delta):
        flow_max = self._local_max.setdefault((client_id, gateway_id), {})
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

    def _state_as_delta(self, client_id, gateway_id):
        return {
            from_bank: [bank_max.from_account, bank_max.amount]
            for from_bank, bank_max in self._local_max.get(
                (client_id, gateway_id), {}
            ).items()
        }

    def _flush_data(self, client_id, gateway_id):
        flow_max = self._local_max.pop((client_id, gateway_id), {})
        self._flush_sharded(
            self._output_exchange,
            internal.MsgType.BANK_MAX_PARTIAL_BATCH,
            client_id,
            gateway_id,
            list(flow_max.values()),
            key_of=lambda bank_max: bank_max.from_bank,
            num_shards=self.config.num_shards,
            batch_size=self.config.batch_size,
        )

    def _send_final_eof(self, client_id, gateway_id, eof):
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.EOF,
                client_id,
                gateway_id,
                eof,
                message_id=final_eof_id(client_id, gateway_id, eof),
            ),
            routing_key=self._shard_routing_key(0),
        )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [BankMaxAggregator] %(levelname)s %(message)s",
    )
    config = Config()
    aggregator = BankMaxAggregator(config)
    try:
        aggregator.start()
    except Exception as e:
        logging.error(f"Error during BankMaxAggregator execution: {e}")
    finally:
        aggregator.shutdown()


if __name__ == "__main__":
    main()
