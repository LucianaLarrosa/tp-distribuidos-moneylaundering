import logging

from common.communication.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeDirectRabbitMQ,
)
from common.idempotency.ids import eof_id, final_eof_id
from common.models.query_results import Q4Result
from common.communication.protocol import internal
from common.worker.stateful_worker import StatefulWorker
from config import Config


class PathFrequencyFilter(StatefulWorker):
    def __init__(self, config):
        super().__init__(config)
        self._paths = (
            {}
        )  # (client_id) -> {(from_bank, from_account, to_bank, to_account): set((mid_bank, mid_account))}

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

    def _flush_data(self, client_id):
        paths = self._paths.pop(client_id, {})
        self._flush_sharded(
            self._output_exchange,
            internal.MsgType.Q4_RESULT_BATCH,
            client_id,
            self._create_results(paths),
            key_of=lambda result: f"{result.bank}.{result.account}",
            num_shards=self.config.output_node_count,
            batch_size=self.config.batch_size,
            routing_key_for=lambda shard: f"{self.config.output_node_prefix}{shard}",
        )

    def _send_final_eof(self, client_id, eof):
        self._output_exchange.send(
            internal.serialize_msg(
                internal.MsgType.EOF,
                client_id,
                eof,
                message_id=final_eof_id(client_id, eof),
            ),
            routing_key=f"{self.config.output_node_prefix}0",
        )

    def _add_path(
        self,
        client_id,
        from_bank,
        from_account,
        to_bank,
        to_account,
        mid_bank,
        mid_account,
    ):
        self._paths.setdefault(client_id, {}).setdefault(
            (from_bank, from_account, to_bank, to_account), set()
        ).add((mid_bank, mid_account))

    def _handle_data_message(self, _, client_id, payload):
        super()._handle_data_message(_, client_id, payload)
        delta = [
            [p.from_bank, p.from_account, p.to_bank, p.to_account, p.mid_bank, p.mid_account]
            for p in payload
        ]
        self._apply_delta(client_id, delta)
        return delta

    def _cleanup_state(self, client_id):
        super()._cleanup_state(client_id)
        self._paths.pop(client_id, None)

    def _apply_delta(self, client_id, delta):
        for from_bank, from_account, to_bank, to_account, mid_bank, mid_account in delta:
            self._add_path(
                client_id,
                from_bank,
                from_account,
                to_bank,
                to_account,
                mid_bank,
                mid_account,
            )

    def _state_as_delta(self, client_id):
        out = []
        for (
            from_bank,
            from_account,
            to_bank,
            to_account,
        ), mid_accounts in self._paths.get(client_id, {}).items():
            for mid_bank, mid_account in mid_accounts:
                out.append(
                    [from_bank, from_account, to_bank, to_account, mid_bank, mid_account]
                )
        return out


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
