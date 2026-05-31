import hashlib
import logging
import random

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.models.path import Path
from common.protocol import internal
from common.worker.sent_coordinated_worker import SentCoordinatedWorker
from config import Config


class PathMapper(SentCoordinatedWorker):
    def __init__(self, config):
        self.config = config
        super().__init__()
        self._account_edges = (
            {}
        )  # (client_id, gateway_id) -> {(bank, account): (in_accounts, out_accounts)}

        self._input_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            routing_keys=[self._get_ring_routing_key(config.node_id)],
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

    def _shard_key(self, bank, account, other_bank, other_account):
        node_id = (
            int(
                hashlib.md5(
                    f"{bank}.{account}.{other_bank}.{other_account}".encode()
                ).hexdigest(),
                16,
            )
            % self.config.output_node_count
        )
        return f"{self.config.output_node_prefix}{node_id}"

    def _create_paths_by_routing_key(self, account_edges):
        """
        Create a dictionary mapping routing keys to paths, when possible, by hashing the bank and account of both extremes to ensure account affinity.
        """
        paths_by_routing_key = {}
        for (bank, account), (in_accounts, out_accounts) in account_edges.items():
            if not in_accounts or not out_accounts:
                continue
            for from_bank, from_account in in_accounts:
                for to_bank, to_account in out_accounts:
                    if (from_bank, from_account) == (to_bank, to_account):
                        continue
                    paths_by_routing_key.setdefault(
                        self._shard_key(from_bank, from_account, to_bank, to_account),
                        [],
                    ).append(
                        Path(
                            from_bank=from_bank,
                            from_account=from_account,
                            mid_bank=bank,
                            mid_account=account,
                            to_bank=to_bank,
                            to_account=to_account,
                        )
                    )
        return paths_by_routing_key

    def _flush_data_in_batches(self, client_id, gateway_id, routing_key, paths):
        """
        Flush the given paths in batches with the specified routing key.
        """
        for i in range(0, len(paths), self.config.batch_size):
            self._output_exchange.send(
                internal.serialize_msg(
                    internal.MsgType.PATH_BATCH,
                    client_id,
                    gateway_id,
                    paths[i : i + self.config.batch_size],
                ),
                routing_key=routing_key,
            )
            self._increment_sent_count(client_id, gateway_id)

    def _flush_data(self, client_id, gateway_id):
        account_edges = self._account_edges.pop((client_id, gateway_id), {})
        paths_by_routing_key = self._create_paths_by_routing_key(account_edges)
        for routing_key, paths in paths_by_routing_key.items():
            self._flush_data_in_batches(client_id, gateway_id, routing_key, paths)

    def _send_final_eof(self, client_id, gateway_id, eof):
        """
        Send the final EOF to a randomly selected output node of the next stage.
        """
        node_id = random.randint(0, self.config.output_node_count - 1)
        self._output_exchange.send(
            internal.serialize_msg(internal.MsgType.EOF, client_id, gateway_id, eof),
            routing_key=f"{self.config.output_node_prefix}{node_id}",
        )

    def _update_account_edges(self, client_id, gateway_id, edge):
        """
        Update the account edges by adding the other account to the appropriate in or out set based on whether the edge is a sender or receiver.
        """
        client_gateway_key = (client_id, gateway_id)
        account_key = (edge.bank, edge.account)
        other_account_key = (edge.other_bank, edge.other_account)
        in_accounts, out_accounts = self._account_edges.setdefault(
            client_gateway_key, {}
        ).setdefault(account_key, (set(), set()))
        if edge.is_sender:
            out_accounts.add(other_account_key)
        else:
            in_accounts.add(other_account_key)

    def _handle_data_message(self, _, client_id, gateway_id, payload):
        for edge in payload:
            self._update_account_edges(client_id, gateway_id, edge)
        super()._handle_data_message(_, client_id, gateway_id, payload)


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [PathMapper] %(levelname)s %(message)s"
    )
    config = Config()
    worker = PathMapper(config)
    try:
        worker.start()
    except Exception as e:
        logging.error(f"Error during PathMapper execution: {e}")
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
