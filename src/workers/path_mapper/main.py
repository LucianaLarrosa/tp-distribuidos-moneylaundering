import logging
import random

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.models.path import Path
from common.protocol.internal import internal
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from config import Config


class PathMapper(StatefulCoordinatedWorker):
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

    def _create_paths(self, account_edges):
        """
        Build the in × out cartesian-product paths through each mid account,
        flattened into a single list (the flush helper shards them by the path
        extremes (from, to) to ensure account affinity downstream).
        """
        paths = []
        for (bank, account), (in_accounts, out_accounts) in account_edges.items():
            if not in_accounts or not out_accounts:
                continue
            for from_bank, from_account in in_accounts:
                for to_bank, to_account in out_accounts:
                    if (from_bank, from_account) == (to_bank, to_account):
                        continue
                    paths.append(
                        Path(
                            from_bank=from_bank,
                            from_account=from_account,
                            mid_bank=bank,
                            mid_account=account,
                            to_bank=to_bank,
                            to_account=to_account,
                        )
                    )
        return paths

    def _flush_data(self, client_id, gateway_id):
        account_edges = self._account_edges.pop((client_id, gateway_id), {})
        self._flush_sharded(
            self._output_exchange,
            internal.MsgType.PATH_BATCH,
            client_id,
            gateway_id,
            self._create_paths(account_edges),
            key_of=lambda path: f"{path.from_bank}.{path.from_account}.{path.to_bank}.{path.to_account}",
            num_shards=self.config.output_node_count,
            batch_size=self.config.batch_size,
            routing_key_for=lambda shard: f"{self.config.output_node_prefix}{shard}",
        )

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
