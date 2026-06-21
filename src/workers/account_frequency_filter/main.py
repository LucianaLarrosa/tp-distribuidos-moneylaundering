import logging

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.ids import eof_id, final_eof_id
from common.models.account_edge import AccountEdge
from common.protocol.internal import internal
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from config import Config


class AccountFrequencyFilter(StatefulCoordinatedWorker):
    def __init__(self, config):
        super().__init__(config)
        self._account_edges = (
            {}
        )  # (client_id, gateway_id) -> {(bank, account): (in_accounts, out_accounts)}

        self._input_exchange = MessageMiddlewareExchangeDirectRabbitMQ(
            host=config.rabbitmq_host,
            exchange_name=config.input_exchange,
            routing_keys=[self._get_ring_routing_key(config.node_id)],
            queue_name=f"{config.input_exchange}_{config.node_id}",
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
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.EOF,
                client_id,
                gateway_id,
                eof,
                message_id=final_eof_id(client_id, gateway_id, eof),
            ),
            routing_key=f"{self.config.output_node_prefix}0",
        )

    def _add_edge(
        self, client_id, gateway_id, bank, account, other_bank, other_account, is_sender
    ):
        in_accounts, out_accounts = self._account_edges.setdefault(
            (client_id, gateway_id), {}
        ).setdefault((bank, account), (set(), set()))
        if is_sender:
            out_accounts.add((other_bank, other_account))
        else:
            in_accounts.add((other_bank, other_account))

    def _handle_data_message(self, _, client_id, gateway_id, payload):
        super()._handle_data_message(_, client_id, gateway_id, payload)
        delta = [
            [e.bank, e.account, e.other_bank, e.other_account, e.is_sender]
            for e in payload
        ]
        self._apply_delta(client_id, gateway_id, delta)
        return delta

    def _cleanup_state(self, client_id, gateway_id):
        super()._cleanup_state(client_id, gateway_id)
        self._account_edges.pop((client_id, gateway_id), None)

    def _apply_delta(self, client_id, gateway_id, delta):
        for bank, account, other_bank, other_account, is_sender in delta:
            self._add_edge(
                client_id, gateway_id, bank, account, other_bank, other_account, is_sender
            )

    def _state_as_delta(self, client_id, gateway_id):
        edges = []
        for (bank, account), (in_accounts, out_accounts) in self._account_edges.get(
            (client_id, gateway_id), {}
        ).items():
            for other_bank, other_account in in_accounts:
                edges.append([bank, account, other_bank, other_account, False])
            for other_bank, other_account in out_accounts:
                edges.append([bank, account, other_bank, other_account, True])
        return edges


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
