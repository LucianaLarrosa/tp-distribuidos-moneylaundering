import logging

from common.communication.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.idempotency.ids import eof_id, final_eof_id
from common.models.account_edge import AccountEdge
from common.communication.protocol import internal
from common.worker.sharding import shard_of
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from common.worker.safe_output_capable import SafeOutputCapable
from config import Config


class BidirectionalSharder(SafeOutputCapable, StatefulCoordinatedWorker):
    NUM_FIELDS_FOR_SHARDING = 2

    def __init__(self, config):
        super().__init__(config)

        self._input_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
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
        self._control_output_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
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

    @property
    def _control_output_middleware(self):
        return self._control_output_exchange

    def _shard_for(self, bank, account):
        return shard_of(f"{bank}.{account}", self.config.output_node_count)

    def _flush_data(self, _client_id):
        pass

    def _send_final_eof(self, client_id, eof):
        self._control_output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.EOF,
                client_id,
                eof,
                message_id=final_eof_id(client_id, eof),
            ),
            routing_key=f"{self.config.output_node_prefix}0",
        )

    def _create_edges_by_shard(self, payload):
        edges_by_shard = {}
        for transaction in payload:
            if None in (
                transaction.from_bank,
                transaction.from_account,
                transaction.to_bank,
                transaction.to_account,
            ):
                continue
            for transaction_direction in [
                (
                    transaction.from_bank,
                    transaction.from_account,
                    transaction.to_bank,
                    transaction.to_account,
                    True,
                ),
                (
                    transaction.to_bank,
                    transaction.to_account,
                    transaction.from_bank,
                    transaction.from_account,
                    False,
                ),
            ]:
                shard = self._shard_for(
                    *transaction_direction[: self.NUM_FIELDS_FOR_SHARDING]
                )
                edges_by_shard.setdefault(shard, []).append(
                    AccountEdge(*transaction_direction)
                )
        return edges_by_shard

    def _handle_data_message(self, msg_type, client_id, payload):
        sent = 0
        for shard, edges in self._create_edges_by_shard(payload).items():
            self._send(
                self._output_exchange,
                internal.MsgType.ACCOUNT_EDGE_BATCH,
                client_id,
                edges,
                routing_key=f"{self.config.output_node_prefix}{shard}",
            )
            self._increment_sent_count(client_id)
            sent += 1
        super()._handle_data_message(msg_type, client_id, payload)
        return {"sent": sent}

    def _apply_delta(self, client_id, delta):
        with self._sent_count_lock:
            self._sent_count[client_id] = (
                self._sent_count.get(client_id, 0) + delta["sent"]
            )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [BidirectionalSharder] %(levelname)s %(message)s",
    )
    config = Config()
    worker = BidirectionalSharder(config)
    try:
        worker.start()
    except Exception as e:
        logging.error(f"Error during BidirectionalSharder execution: {e}")
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
