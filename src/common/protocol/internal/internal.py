from datetime import datetime

from . import internal_pb2 as pb

from common.models.raw_transaction import RawTransaction
from common.models.raw_account import RawAccount
from common.models.transaction import Transaction
from common.models.bank import Bank
from common.models.query_results import Q1Result, Q2Result, Q3Result, Q4Result, Q5Result
from common.models.transaction_for_currency_conversion import (
    TransactionForCurrencyConversion,
)
from common.models.transaction_amount import TransactionAmount
from common.models.count import Count
from common.models.bank_max_partial import BankMaxPartial
from common.models.payment_format_partial import PaymentFormatPartial
from common.models.payment_format_average import PaymentFormatAverage
from common.models.eof import EOF, RingEOF
from common.models.account_edge import AccountEdge
from common.models.path import Path


class MsgType:
    Q1_RESULT_BATCH = 1
    Q2_RESULT_BATCH = 2
    Q3_RESULT_BATCH = 3
    Q4_RESULT_BATCH = 4
    Q5_RESULT_BATCH = 5
    RAW_TRANSACTION_BATCH = 6
    RAW_ACCOUNT_BATCH = 7
    TRANSACTION_BATCH = 8
    BANK_BATCH = 9
    QUERY_END = 10
    CURRENCY_CONVERSION_BATCH = 11
    AMOUNT_TRANSACTION_BATCH = 12
    COUNT = 13
    EOF = 14
    RING_EOF = 15
    BANK_MAX_PARTIAL_BATCH = 16
    PAYMENT_FORMAT_PARTIAL_BATCH = 17
    PAYMENT_FORMAT_AVERAGE_BATCH = 18
    ACCOUNT_EDGE_BATCH = 19
    PATH_BATCH = 20


# ---------- API ----------


def serialize_msg(msg_type, client_id, gateway_id, *args):
    env = pb.Envelope(client_id=client_id, gateway_id=gateway_id)
    SERIALIZERS[msg_type](env, *args)
    return env.SerializeToString()


def deserialize_msg(data):
    env = pb.Envelope()
    env.ParseFromString(data)
    payload_type = TYPES[env.WhichOneof("payload")]
    payload = DESERIALIZERS[payload_type](env)
    return payload_type, env.client_id, env.gateway_id, payload


# ---------- traducción de batches ----------

BATCH_DATA = {
    MsgType.RAW_TRANSACTION_BATCH: (RawTransaction, "raw_transaction_batch", ["raw"]),
    MsgType.RAW_ACCOUNT_BATCH: (RawAccount, "raw_account_batch", ["raw"]),
    MsgType.BANK_BATCH: (Bank, "bank_batch", ["bank_id", "name"]),
    MsgType.CURRENCY_CONVERSION_BATCH: (
        TransactionForCurrencyConversion,
        "currency_conversion_batch",
        ["timestamp", "amount", "currency"],
    ),
    MsgType.AMOUNT_TRANSACTION_BATCH: (
        TransactionAmount,
        "amount_transaction_batch",
        ["amount"],
    ),
    MsgType.BANK_MAX_PARTIAL_BATCH: (
        BankMaxPartial,
        "bank_max_partial_batch",
        ["from_bank", "from_account", "amount"],
    ),
    MsgType.PAYMENT_FORMAT_PARTIAL_BATCH: (
        PaymentFormatPartial,
        "payment_format_partial_batch",
        ["payment_format", "total_amount", "count"],
    ),
    MsgType.PAYMENT_FORMAT_AVERAGE_BATCH: (
        PaymentFormatAverage,
        "payment_format_average_batch",
        ["payment_format", "average_amount"],
    ),
    MsgType.ACCOUNT_EDGE_BATCH: (
        AccountEdge,
        "account_edge_batch",
        ["bank", "account", "other_bank", "other_account", "is_sender"],
    ),
    MsgType.PATH_BATCH: (
        Path,
        "path_batch",
        [
            "from_bank",
            "from_account",
            "mid_bank",
            "mid_account",
            "to_bank",
            "to_account",
        ],
    ),
    MsgType.Q1_RESULT_BATCH: (
        Q1Result,
        "q1_result_batch",
        ["from_bank", "from_account", "to_bank", "to_account", "amount_paid"],
    ),
    MsgType.Q2_RESULT_BATCH: (
        Q2Result,
        "q2_result_batch",
        ["bank_name", "from_account", "amount_paid"],
    ),
    MsgType.Q3_RESULT_BATCH: (
        Q3Result,
        "q3_result_batch",
        ["from_bank", "from_account", "amount_paid"],
    ),
    MsgType.Q4_RESULT_BATCH: (Q4Result, "q4_result_batch", ["bank", "account"]),
    MsgType.Q5_RESULT_BATCH: (Q5Result, "q5_result_batch", ["count"]),
}


def _serialize_batch(data, env, batch):
    _, field_name, fields = data
    payload = getattr(env, field_name)
    payload.SetInParent()
    for elem in batch:
        item = payload.items.add()
        for field in fields:
            setattr(item, field, getattr(elem, field))


def _deserialize_batch(data, env):
    msg_type_class, field_name, fields = data
    payload = getattr(env, field_name)
    return [
        msg_type_class(**{field: getattr(item, field) for field in fields})
        for item in payload.items
    ]


# ---------- casos especiales ----------


def _serialize_transaction_batch(env, transactions):
    env.transaction_batch.SetInParent()
    for tx in transactions:
        item = env.transaction_batch.items.add()
        item.timestamp = tx.timestamp.isoformat()
        item.from_bank = tx.from_bank
        item.from_account = tx.from_account
        item.to_bank = tx.to_bank
        item.to_account = tx.to_account
        item.amount = tx.amount
        item.currency = tx.currency
        item.payment_format = tx.payment_format


def _deserialize_transaction_batch(env):
    return [
        Transaction(
            timestamp=datetime.fromisoformat(item.timestamp),
            from_bank=item.from_bank,
            from_account=item.from_account,
            to_bank=item.to_bank,
            to_account=item.to_account,
            amount=item.amount,
            currency=item.currency,
            payment_format=item.payment_format,
        )
        for item in env.transaction_batch.items
    ]


def _serialize_count(env, count):
    env.count.count = count.count


def _deserialize_count(env):
    return Count(count=env.count.count)


def _serialize_eof(env, eof):
    env.eof.message_count = eof.message_count


def _deserialize_eof(env):
    return EOF(message_count=env.eof.message_count)


def _serialize_ring_eof(env, ring_eof):
    r = env.ring_eof
    r.expected_count = ring_eof.expected_count
    r.total_processed_count = ring_eof.total_processed_count
    if ring_eof.coordinator_id is not None:
        r.coordinator_id = ring_eof.coordinator_id
    if ring_eof.total_sent_count is not None:
        r.total_sent_count = ring_eof.total_sent_count


def _deserialize_ring_eof(env):
    r = env.ring_eof
    return RingEOF(
        expected_count=r.expected_count,
        total_processed_count=r.total_processed_count,
        coordinator_id=r.coordinator_id if r.HasField("coordinator_id") else None,
        total_sent_count=r.total_sent_count if r.HasField("total_sent_count") else None,
    )


def _serialize_query_end(env, query_id, message_count):
    env.query_end.query_id = query_id
    env.query_end.message_count = message_count


def _deserialize_query_end(env):
    return env.query_end.query_id, env.query_end.message_count


# ---------- tablas para mapear ----------

SERIALIZERS = {
    MsgType.TRANSACTION_BATCH: _serialize_transaction_batch,
    MsgType.COUNT: _serialize_count,
    MsgType.EOF: _serialize_eof,
    MsgType.RING_EOF: _serialize_ring_eof,
    MsgType.QUERY_END: _serialize_query_end,
}

DESERIALIZERS = {
    MsgType.TRANSACTION_BATCH: _deserialize_transaction_batch,
    MsgType.COUNT: _deserialize_count,
    MsgType.EOF: _deserialize_eof,
    MsgType.RING_EOF: _deserialize_ring_eof,
    MsgType.QUERY_END: _deserialize_query_end,
}

TYPES = {
    "transaction_batch": MsgType.TRANSACTION_BATCH,
    "count": MsgType.COUNT,
    "eof": MsgType.EOF,
    "ring_eof": MsgType.RING_EOF,
    "query_end": MsgType.QUERY_END,
}

for msg_type, batch_data in BATCH_DATA.items():
    batch_field = batch_data[1]
    SERIALIZERS[msg_type] = lambda env, batch, batch_data=batch_data: _serialize_batch(
        batch_data, env, batch
    )
    DESERIALIZERS[msg_type] = lambda env, batch_data=batch_data: _deserialize_batch(
        batch_data, env
    )
    TYPES[batch_field] = msg_type
