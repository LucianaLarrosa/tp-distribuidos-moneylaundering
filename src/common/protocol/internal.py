import json
from dataclasses import asdict
from datetime import datetime

from common.models.raw_transaction import RawTransaction
from common.models.raw_account import RawAccount
from common.models.transaction import Transaction
from common.models.bank import Bank
from common.models.query_results import Q2Result, Q5Result
from common.models.transaction_for_currency_conversion import (
    TransactionForCurrencyConversion,
)
from common.models.transaction_amount import TransactionAmount
from common.models.count import Count
from common.models.bank_max_partial import BankMaxPartial
from common.models.eof import EOF, RingEOF
from common.models.account_edge import AccountEdge


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
    ACCOUNT_EDGE_BATCH = 17


# ---------- API ----------


def serialize_msg(msg_type, client_id, gateway_id, *args):
    handler = SERIALIZERS[msg_type]
    payload = handler(*args)
    return json.dumps(
        {
            "type": msg_type,
            "client_id": client_id,
            "gateway_id": gateway_id,
            "payload": payload,
        }
    ).encode("utf-8")


def deserialize_msg(data):
    obj = json.loads(data.decode("utf-8"))
    msg_type = obj["type"]
    client_id = obj["client_id"]
    gateway_id = obj["gateway_id"]
    handler = DESERIALIZERS[msg_type]
    payload = handler(obj.get("payload"))
    return msg_type, client_id, gateway_id, payload


# ---------- handlers serialize / deserialize por tipo de mensaje ----------


def _serialize_batch(items):
    return [asdict(item) for item in items]


def _serialize_transaction_batch(transactions):
    return [
        {**asdict(tx), "timestamp": tx.timestamp.isoformat()} for tx in transactions
    ]


def _serialize_eof(eof):
    return asdict(eof)


def _serialize_ring_eof(ring_eof):
    return asdict(ring_eof)


def _serialize_currency_conversion_batch(transactions):
    return [asdict(tx) for tx in transactions]


def _serialize_amount_transaction_batch(transactions):
    return [asdict(tx) for tx in transactions]


def _serialize_count(count):
    return asdict(count)


def _serialize_query_end(query_id, message_count):
    return {"query_id": query_id, "message_count": message_count}


def _serialize_account_edge_batch(batch):
    return [asdict(x) for x in batch]


def _deserialize_batch(cls, payload):
    return [cls(**item) for item in payload]


def _deserialize_raw_transaction_batch(payload):
    return _deserialize_batch(RawTransaction, payload)


def _deserialize_raw_account_batch(payload):
    return _deserialize_batch(RawAccount, payload)


def _deserialize_bank_batch(payload):
    return _deserialize_batch(Bank, payload)


def _deserialize_bank_max_partial_batch(payload):
    return _deserialize_batch(BankMaxPartial, payload)


def _deserialize_q2_result_batch(payload):
    return _deserialize_batch(Q2Result, payload)


def _deserialize_q5_result_batch(payload):
    return _deserialize_batch(Q5Result, payload)


def _deserialize_transaction_batch(payload):
    return [
        Transaction(**{**tx, "timestamp": datetime.fromisoformat(tx["timestamp"])})
        for tx in payload
    ]


def _deserialize_currency_conversion_batch(payload):
    return [TransactionForCurrencyConversion(**tx) for tx in payload]


def _deserialize_amount_transaction_batch(payload):
    return [TransactionAmount(**tx) for tx in payload]


def _deserialize_count(payload):
    return Count(**payload)


def _deserialize_query_end(payload):
    return payload["query_id"], payload["message_count"]


def _deserialize_eof(payload):
    return EOF(**payload)


def _deserialize_ring_eof(payload):
    return RingEOF(**payload)


def _deserialize_account_edge_batch(payload):
    return [AccountEdge(**x) for x in payload]


SERIALIZERS = {
    MsgType.RAW_TRANSACTION_BATCH: _serialize_batch,
    MsgType.RAW_ACCOUNT_BATCH: _serialize_batch,
    MsgType.TRANSACTION_BATCH: _serialize_transaction_batch,
    MsgType.BANK_BATCH: _serialize_batch,
    MsgType.Q2_RESULT_BATCH: _serialize_batch,
    MsgType.Q5_RESULT_BATCH: _serialize_batch,
    MsgType.QUERY_END: _serialize_query_end,
    MsgType.CURRENCY_CONVERSION_BATCH: _serialize_currency_conversion_batch,
    MsgType.AMOUNT_TRANSACTION_BATCH: _serialize_amount_transaction_batch,
    MsgType.COUNT: _serialize_count,
    MsgType.BANK_MAX_PARTIAL_BATCH: _serialize_batch,
    MsgType.EOF: _serialize_eof,
    MsgType.RING_EOF: _serialize_ring_eof,
    MsgType.ACCOUNT_EDGE_BATCH: _serialize_account_edge_batch,
}

DESERIALIZERS = {
    MsgType.RAW_TRANSACTION_BATCH: _deserialize_raw_transaction_batch,
    MsgType.RAW_ACCOUNT_BATCH: _deserialize_raw_account_batch,
    MsgType.TRANSACTION_BATCH: _deserialize_transaction_batch,
    MsgType.BANK_BATCH: _deserialize_bank_batch,
    MsgType.Q2_RESULT_BATCH: _deserialize_q2_result_batch,
    MsgType.Q5_RESULT_BATCH: _deserialize_q5_result_batch,
    MsgType.QUERY_END: _deserialize_query_end,
    MsgType.CURRENCY_CONVERSION_BATCH: _deserialize_currency_conversion_batch,
    MsgType.AMOUNT_TRANSACTION_BATCH: _deserialize_amount_transaction_batch,
    MsgType.COUNT: _deserialize_count,
    MsgType.BANK_MAX_PARTIAL_BATCH: _deserialize_bank_max_partial_batch,
    MsgType.EOF: _deserialize_eof,
    MsgType.RING_EOF: _deserialize_ring_eof,
    MsgType.ACCOUNT_EDGE_BATCH: _deserialize_account_edge_batch,
}
