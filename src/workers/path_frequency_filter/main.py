import logging
import random

from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.models.query_results import Q4Result
from common.protocol.internal import internal
from common.worker.stateful_coordinated_worker import StatefulCoordinatedWorker
from config import Config


class PathFrequencyFilter(StatefulCoordinatedWorker):
    def __init__(self, config):
        self.config = config
        super().__init__()
        self._paths = (
            {}
        )  # (client_id, gateway_id) -> {(from_bank, from_account, to_bank, to_account): set((mid_bank, mid_account))}

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

    def _create_results(self, paths):
        """
        Build the Q4Results for paths above the frequency threshold, flattened
        into a single list (the flush helper shards them by (bank, account)).
        """
        results = []
        for (
            from_bank,
            from_account,
            to_bank,
            to_account,
        ), mid_accounts in paths.items():
            if len(mid_accounts) < self.config.min_required_accounts:
                continue
            for bank, account in [(from_bank, from_account), (to_bank, to_account)]:
                results.append(Q4Result(bank, account))
        return results

    def _flush_data(self, client_id, gateway_id):
        paths = self._paths.pop((client_id, gateway_id), {})
        self._flush_sharded(
            self._output_exchange,
            internal.MsgType.Q4_RESULT_BATCH,
            client_id,
            gateway_id,
            self._create_results(paths),
            key_of=lambda result: f"{result.bank}.{result.account}",
            num_shards=self.config.output_node_count,
            batch_size=self.config.batch_size,
            routing_key_for=lambda shard: f"{self.config.output_node_prefix}{shard}",
        )

    def _send_final_eof(self, client_id, gateway_id, eof):
        node_id = random.randint(0, self.config.output_node_count - 1)
        self._output_exchange.send(
            internal.serialize_msg(internal.MsgType.EOF, client_id, gateway_id, eof),
            routing_key=f"{self.config.output_node_prefix}{node_id}",
        )

    def _update_paths(self, client_id, gateway_id, path):
        """
        Update the paths by adding the intermediary account to the set of intermediaries.
        """
        client_gateway_key = (client_id, gateway_id)
        extreme_key = (
            path.from_bank,
            path.from_account,
            path.to_bank,
            path.to_account,
        )
        mid_account_key = (path.mid_bank, path.mid_account)
        self._paths.setdefault(client_gateway_key, {}).setdefault(
            extreme_key, set()
        ).add((mid_account_key))

    def _handle_data_message(self, _, client_id, gateway_id, payload):
        for path in payload:
            self._update_paths(client_id, gateway_id, path)
        super()._handle_data_message(_, client_id, gateway_id, payload)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [PathFrequencyFilter] %(levelname)s %(message)s",
    )
    config = Config()
    worker = PathFrequencyFilter(config)
    try:
        worker.start()
    except Exception as e:
        logging.error(f"Error during PathFrequencyFilter execution: {e}")
    finally:
        worker.shutdown()


if __name__ == "__main__":
    main()
