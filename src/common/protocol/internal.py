import json
from dataclasses import asdict
from datetime import datetime

from common.models.raw_transaction import RawTransaction
from common.models.raw_account import RawAccount
from common.models.transaction import Transaction
from common.models.bank import Bank
from common.models.bank_max_partial import BankMaxPartial
from common.models.query_results import Q2Result
from common.models.eof import EOF, RingEOF


class MsgType:
    RAW_TRANSACTION_BATCH = "raw_transaction_batch"
    RAW_ACCOUNT_BATCH = "raw_account_batch"
    TRANSACTION_BATCH = "transaction_batch"
    BANK_BATCH = "bank_batch"
    BANK_MAX_PARTIAL_BATCH = "bank_max_partial_batch"
    Q2_RESULT_BATCH = "q2_result_batch"
    EOF = "eof"
    RING_EOF = "ring_eof"


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


def _deserialize_transaction_batch(payload):
    return [
        Transaction(**{**tx, "timestamp": datetime.fromisoformat(tx["timestamp"])})
        for tx in payload
    ]


def _deserialize_eof(payload):
    return EOF(**payload)


def _deserialize_ring_eof(payload):
    return RingEOF(**payload)


SERIALIZERS = {
    MsgType.RAW_TRANSACTION_BATCH: _serialize_batch,
    MsgType.RAW_ACCOUNT_BATCH: _serialize_batch,
    MsgType.TRANSACTION_BATCH: _serialize_transaction_batch,
    MsgType.BANK_BATCH: _serialize_batch,
    MsgType.BANK_MAX_PARTIAL_BATCH: _serialize_batch,
    MsgType.Q2_RESULT_BATCH: _serialize_batch,
    MsgType.EOF: _serialize_eof,
    MsgType.RING_EOF: _serialize_ring_eof,
}

DESERIALIZERS = {
    MsgType.RAW_TRANSACTION_BATCH: _deserialize_raw_transaction_batch,
    MsgType.RAW_ACCOUNT_BATCH: _deserialize_raw_account_batch,
    MsgType.TRANSACTION_BATCH: _deserialize_transaction_batch,
    MsgType.BANK_BATCH: _deserialize_bank_batch,
    MsgType.BANK_MAX_PARTIAL_BATCH: _deserialize_bank_max_partial_batch,
    MsgType.Q2_RESULT_BATCH: _deserialize_q2_result_batch,
    MsgType.EOF: _deserialize_eof,
    MsgType.RING_EOF: _deserialize_ring_eof,
}
