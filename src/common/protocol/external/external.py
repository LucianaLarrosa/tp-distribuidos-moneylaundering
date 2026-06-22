from common.models.raw_transaction import RawTransaction
from common.models.raw_account import RawAccount
from common.models.query_results import (
    Q1Result,
    Q2Result,
    Q3Result,
    Q4Result,
    Q5Result,
)
from common.protocol.length_prefix import (
    UINT32_SIZE,
    deserialize_uint32,
    serialize_uint32,
)

from . import external_pb2 as pb


class MsgType:
    TRANSACTION_BATCH = 1  # (client -> gateway)
    ACCOUNT_BATCH = 2  # (client -> gateway)
    EOF_TRANSACTIONS = 3  # (client -> gateway)
    EOF_ACCOUNTS = 4  # (client -> gateway)
    QUERY_RESULT = 5  # (gateway -> client)
    QUERY_END = 6  # (gateway -> client)
    REDIRECT = 7  # (proxy -> client)
    ACK = 8  # (gateway -> client)
    ANNOUNCE = 9  # (client -> gateway)


# ---------- API ----------


def send_msg(sock, msg_type, *args, client_id="", message_id=""):
    env = pb.Envelope(client_id=client_id, message_id=message_id)
    SERIALIZERS[msg_type](env, *args)
    data = env.SerializeToString()
    sock.send(serialize_uint32(len(data)) + data)


def recv_msg(sock):
    size = deserialize_uint32(sock.recv(UINT32_SIZE))
    env = pb.Envelope()
    env.ParseFromString(sock.recv(size))
    payload_type = TYPES[env.WhichOneof("payload")]
    return payload_type, DESERIALIZERS[payload_type](env)


# ---------- traducción de batches ----------


def _serialize_batch(repeated, records, fields):
    for record in records:
        item = repeated.add()
        for field in fields:
            setattr(item, field, getattr(record, field))


def _deserialize_batch(repeated, record_class, fields):
    return [
        record_class(**{field: getattr(item, field) for field in fields})
        for item in repeated
    ]


QUERY_RESULT_DATA = {
    1: (
        Q1Result,
        "q1",
        ["from_bank", "from_account", "to_bank", "to_account", "amount_paid"],
    ),
    2: (Q2Result, "q2", ["bank_name", "from_account", "amount_paid"]),
    3: (Q3Result, "q3", ["from_bank", "from_account", "amount_paid"]),
    4: (Q4Result, "q4", ["bank", "account"]),
    5: (Q5Result, "q5", ["count"]),
}


# ---------- serializers ----------


def _serialize_transaction_batch(env, transactions):
    env.transaction_batch.SetInParent()
    _serialize_batch(env.transaction_batch.items, transactions, ["raw"])


def _serialize_account_batch(env, accounts):
    env.account_batch.SetInParent()
    _serialize_batch(env.account_batch.items, accounts, ["raw"])


def _serialize_eof_transactions(env):
    env.eof_transactions.SetInParent()


def _serialize_eof_accounts(env):
    env.eof_accounts.SetInParent()


def _serialize_ack(env):
    env.ack.SetInParent()


def _serialize_announce(env):
    env.announce.SetInParent()


def _serialize_query_result(env, query_id, records):
    _, field_name, fields = QUERY_RESULT_DATA[query_id]
    env.query_result.query_id = query_id
    batch = getattr(env.query_result, field_name)
    batch.SetInParent()
    _serialize_batch(batch.items, records, fields)


def _serialize_query_end(env, query_id, message_count):
    env.query_end.query_id = query_id
    env.query_end.message_count = message_count


def _serialize_redirect(env, host, port):
    env.redirect.host = host
    env.redirect.port = port


# ---------- deserializers ----------


def _deserialize_transaction_batch(env):
    records = _deserialize_batch(env.transaction_batch.items, RawTransaction, ["raw"])
    return records, env.client_id, env.message_id


def _deserialize_account_batch(env):
    records = _deserialize_batch(env.account_batch.items, RawAccount, ["raw"])
    return records, env.client_id, env.message_id


def _deserialize_eof_transactions(env):
    return env.client_id, env.message_id


def _deserialize_eof_accounts(env):
    return env.client_id, env.message_id


def _deserialize_none(env):
    return None


def _deserialize_query_result(env):
    qr = env.query_result
    record_class, field_name, fields = QUERY_RESULT_DATA[qr.query_id]
    records = _deserialize_batch(getattr(qr, field_name).items, record_class, fields)
    return qr.query_id, records, env.message_id


def _deserialize_query_end(env):
    return env.query_end.query_id, env.query_end.message_count


def _deserialize_redirect(env):
    return env.redirect.host, env.redirect.port


def _deserialize_announce(env):
    return env.client_id


# ---------- tablas para mapear ----------

SERIALIZERS = {
    MsgType.TRANSACTION_BATCH: _serialize_transaction_batch,
    MsgType.ACCOUNT_BATCH: _serialize_account_batch,
    MsgType.EOF_TRANSACTIONS: _serialize_eof_transactions,
    MsgType.EOF_ACCOUNTS: _serialize_eof_accounts,
    MsgType.QUERY_RESULT: _serialize_query_result,
    MsgType.QUERY_END: _serialize_query_end,
    MsgType.REDIRECT: _serialize_redirect,
    MsgType.ACK: _serialize_ack,
    MsgType.ANNOUNCE: _serialize_announce,
}

DESERIALIZERS = {
    MsgType.TRANSACTION_BATCH: _deserialize_transaction_batch,
    MsgType.ACCOUNT_BATCH: _deserialize_account_batch,
    MsgType.EOF_TRANSACTIONS: _deserialize_eof_transactions,
    MsgType.EOF_ACCOUNTS: _deserialize_eof_accounts,
    MsgType.QUERY_RESULT: _deserialize_query_result,
    MsgType.QUERY_END: _deserialize_query_end,
    MsgType.REDIRECT: _deserialize_redirect,
    MsgType.ACK: _deserialize_none,
    MsgType.ANNOUNCE: _deserialize_announce,
}

TYPES = {
    "transaction_batch": MsgType.TRANSACTION_BATCH,
    "account_batch": MsgType.ACCOUNT_BATCH,
    "eof_transactions": MsgType.EOF_TRANSACTIONS,
    "eof_accounts": MsgType.EOF_ACCOUNTS,
    "query_result": MsgType.QUERY_RESULT,
    "query_end": MsgType.QUERY_END,
    "redirect": MsgType.REDIRECT,
    "ack": MsgType.ACK,
    "announce": MsgType.ANNOUNCE,
}
