import hashlib
import logging
import random

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
    MessageMiddlewareExchangeTopicRabbitMQ,
)
from common.models.account_edge import AccountEdge
from common.protocol import internal
from common.worker.sent_coordinated_worker import SentCoordinatedWorker
from common.worker.safe_output_capable import SafeOutputCapable
from config import Config


class BidirectionalSharder(SentCoordinatedWorker, SafeOutputCapable):
    NUM_FIELDS_FOR_SHARDING = 2

    def __init__(self, config):
        self.config = config
        super().__init__()

        self._input_exchange = MessageMiddlewareExchangeTopicRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            binding_patterns=config.input_routing_keys,
            queue_name=config.input_queue_name,
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

    def _shard_key(self, bank, account):
        node_id = (
            int(hashlib.md5(f"{bank}.{account}".encode()).hexdigest(), 16)
            % self.config.output_node_count
        )
        return f"{self.config.output_node_prefix}{node_id}"

    def _flush_data(self, _client_id, _gateway_id):
        pass

    def _send_final_eof(self, client_id, gateway_id, eof):
        """
        Send the final EOF to a randomly selected output node of the next stage.
        """
        node_id = random.randint(0, self.config.output_node_count - 1)
        self._control_output_exchange.send(
            internal.serialize_msg(internal.MsgType.EOF, client_id, gateway_id, eof),
            routing_key=f"{self.config.output_node_prefix}{node_id}",
        )

    def _create_edges_by_routing_key(self, payload):
        """
        Create a dictionary mapping routing keys to account edges, where each transaction generates sender and receiver edges routed by hashing the bank and account to ensure account affinity.
        """
        edges_by_routing_key = {}
        for transaction in payload:
            if None in (transaction.from_bank, transaction.from_account, transaction.to_bank, transaction.to_account):
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
                edges_by_routing_key.setdefault(
                    self._shard_key(
                        *transaction_direction[: self.NUM_FIELDS_FOR_SHARDING]
                    ),
                    [],
                ).append(AccountEdge(*transaction_direction))
        return edges_by_routing_key

    def _handle_data_message(self, msg_type, client_id, gateway_id, payload):
        """
        Handle a data message by creating account edges for each transaction and sending them with the appropriate routing key.
        """
        edges_by_routing_key = self._create_edges_by_routing_key(payload)
        for routing_key, edges in edges_by_routing_key.items():
            self._output_exchange.send(
                internal.serialize_msg(
                    internal.MsgType.ACCOUNT_EDGE_BATCH, client_id, gateway_id, edges
                ),
                routing_key=routing_key,
            )
            self._increment_sent_count(client_id, gateway_id)
        super()._handle_data_message(msg_type, client_id, gateway_id, payload)


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
