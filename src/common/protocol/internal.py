import json
from dataclasses import asdict

from common.models.raw_transaction import RawTransaction
from common.models.transaction import Transaction
from common.models.transaction_for_currency_conversion import (
    TransactionForCurrencyConversion,
)
from common.models.transaction_amount import TransactionAmount
from common.models.eof import EOF, RingEOF


class MsgType:
    RAW_TRANSACTION_BATCH = "raw_transaction_batch"
    TRANSACTION_BATCH = "transaction_batch"
    CURRENCY_CONVERSION_BATCH = "currency_conversion_batch"
    AMOUNT_TRANSACTION_BATCH = "amount_transaction_batch"
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


def _serialize_raw_transaction_batch(transactions):
    return [asdict(tx) for tx in transactions]


def _serialize_transaction_batch(transactions):
    return [asdict(tx) for tx in transactions]


def _serialize_eof(eof):
    return asdict(eof)


def _serialize_ring_eof(ring_eof):
    return asdict(ring_eof)


def _serialize_currency_conversion_batch(transactions):
    return [asdict(tx) for tx in transactions]


def _serialize_amount_transaction_batch(transactions):
    return [asdict(tx) for tx in transactions]


def _deserialize_raw_transaction_batch(payload):
    return [RawTransaction(**tx) for tx in payload]


def _deserialize_transaction_batch(payload):
    return [Transaction(**tx) for tx in payload]


def _deserialize_currency_conversion_batch(payload):
    return [TransactionForCurrencyConversion(**tx) for tx in payload]


def _deserialize_amount_transaction_batch(payload):
    return [TransactionAmount(**tx) for tx in payload]


def _deserialize_eof(payload):
    return EOF(**payload)


def _deserialize_ring_eof(payload):
    return RingEOF(**payload)


SERIALIZERS = {
    MsgType.RAW_TRANSACTION_BATCH: _serialize_raw_transaction_batch,
    MsgType.TRANSACTION_BATCH: _serialize_transaction_batch,
    MsgType.CURRENCY_CONVERSION_BATCH: _serialize_currency_conversion_batch,
    MsgType.AMOUNT_TRANSACTION_BATCH: _serialize_amount_transaction_batch,
    MsgType.EOF: _serialize_eof,
    MsgType.RING_EOF: _serialize_ring_eof,
}

DESERIALIZERS = {
    MsgType.RAW_TRANSACTION_BATCH: _deserialize_raw_transaction_batch,
    MsgType.TRANSACTION_BATCH: _deserialize_transaction_batch,
    MsgType.CURRENCY_CONVERSION_BATCH: _deserialize_currency_conversion_batch,
    MsgType.AMOUNT_TRANSACTION_BATCH: _deserialize_amount_transaction_batch,
    MsgType.EOF: _deserialize_eof,
    MsgType.RING_EOF: _deserialize_ring_eof,
}
