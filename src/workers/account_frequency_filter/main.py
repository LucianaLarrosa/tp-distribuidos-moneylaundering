import logging
import random

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.models.account_edge import AccountEdge
from common.protocol.internal import internal
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from config import Config


class AccountFrequencyFilter(StatefulCoordinatedWorker):
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

    def _create_edges(self, account_edges):
        """
        Build the account edges that meet the frequency threshold, flattened into
        a single list (the flush helper shards them by (bank, account)).
        """
        edges = []
        for (bank, account), (in_accounts, out_accounts) in account_edges.items():
            for accounts, is_sender in [
                (in_accounts, False),
                (out_accounts, True),
            ]:
                if len(accounts) < self.config.min_required_accounts:
                    continue
                for other_bank, other_account in accounts:
                    edges.append(
                        AccountEdge(
                            bank=other_bank,
                            account=other_account,
                            other_bank=bank,
                            other_account=account,
                            is_sender=not is_sender,
                        )
                    )
        return edges

    def _flush_data(self, client_id, gateway_id):
        account_edges = self._account_edges.pop((client_id, gateway_id), {})
        self._flush_sharded(
            self._output_exchange,
            internal.MsgType.ACCOUNT_EDGE_BATCH,
            client_id,
            gateway_id,
            self._create_edges(account_edges),
            key_of=lambda edge: f"{edge.bank}.{edge.account}",
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
        level=logging.INFO,
        format="%(asctime)s [AccountFrequencyFilter] %(levelname)s %(message)s",
    )
    config = Config()
    worker = AccountFrequencyFilter(config)
    try:
        worker.start()
    except Exception as e:
        logging.error(f"Error during AccountFrequencyFilter execution: {e}")
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
