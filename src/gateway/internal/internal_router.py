from common.protocol.internal import internal
from common.models.eof import EOF


class InternalRouter:
    def __init__(self, exchange, transaction_routing_key, account_routing_key):
        self._exchange = exchange
        self._transaction_rk = transaction_routing_key
        self._account_rk = account_routing_key

    def forward_raw_transactions(self, client_id, gateway_id, batch):
        self._send(
            internal.MsgType.RAW_TRANSACTION_BATCH,
            client_id,
            gateway_id,
            batch,
            self._transaction_rk,
        )

    def forward_raw_accounts(self, client_id, gateway_id, batch):
        self._send(
            internal.MsgType.RAW_ACCOUNT_BATCH,
            client_id,
            gateway_id,
            batch,
            self._account_rk,
        )

    def forward_eof_transactions(self, client_id, gateway_id, batch_count):
        self._send(
            internal.MsgType.EOF,
            client_id,
            gateway_id,
            EOF(message_count=batch_count),
            self._transaction_rk,
        )

    def forward_eof_accounts(self, client_id, gateway_id, batch_count):
        self._send(
            internal.MsgType.EOF,
            client_id,
            gateway_id,
            EOF(message_count=batch_count),
            self._account_rk,
        )

    def _send(self, msg_type, client_id, gateway_id, payload, routing_key):
        msg = internal.serialize_msg(msg_type, client_id, gateway_id, payload)
        self._exchange.send(msg, routing_key=routing_key)
