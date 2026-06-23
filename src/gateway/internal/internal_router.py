from common.ids import root_id, eof_id, final_eof_id
from common.protocol.internal import internal
from common.models.eof import EOF, CLEANUP_EXPECTED_COUNT


class InternalRouter:
    def __init__(self, exchange, transaction_routing_key, account_routing_key):
        self._exchange = exchange
        self._transaction_rk = transaction_routing_key
        self._account_rk = account_routing_key

    def forward_raw_transactions(self, client_id, batch, batch_index):
        self._send(
            internal.MsgType.RAW_TRANSACTION_BATCH,
            client_id,
            batch,
            self._transaction_rk,
            message_id=root_id(client_id, batch_index),
        )

    def forward_raw_accounts(self, client_id, batch, batch_index):
        self._send(
            internal.MsgType.RAW_ACCOUNT_BATCH,
            client_id,
            batch,
            self._account_rk,
            message_id=root_id(client_id, batch_index),
        )

    def forward_eof_transactions(self, client_id, batch_count):
        self._send(
            internal.MsgType.EOF,
            client_id,
            EOF(message_count=batch_count),
            self._transaction_rk,
            message_id=eof_id(client_id),
        )

    def forward_eof_accounts(self, client_id, batch_count):
        self._send(
            internal.MsgType.EOF,
            client_id,
            EOF(message_count=batch_count),
            self._account_rk,
            message_id=eof_id(client_id),
        )

    def forward_cleanup_eof(self, client_id):
        eof = EOF(message_count=CLEANUP_EXPECTED_COUNT)
        message_id = final_eof_id(client_id, eof)
        self._send(
            internal.MsgType.EOF,
            client_id,
            eof,
            self._transaction_rk,
            message_id=message_id,
        )
        self._send(
            internal.MsgType.EOF,
            client_id,
            eof,
            self._account_rk,
            message_id=message_id,
        )

    def _send(self, msg_type, client_id, payload, routing_key, message_id=""):
        msg = internal.serialize_msg(
            msg_type, client_id, payload, message_id=message_id
        )
        self._exchange.send(msg, routing_key=routing_key)
