import json
from dataclasses import asdict

from common.models.raw_transaction import RawTransaction
from common.models.eof import EOF, RingEOF


class MsgType:
    TRANSACTION_BATCH = "transaction_batch"
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


def _serialize_transaction_batch(transactions):
    return [asdict(tx) for tx in transactions]


def _serialize_eof(eof):
    return asdict(eof)


def _serialize_ring_eof(ring_eof):
    return asdict(ring_eof)


def _deserialize_transaction_batch(payload):
    return [RawTransaction(**tx) for tx in payload]


def _deserialize_eof(payload):
    return EOF(**payload)


def _deserialize_ring_eof(payload):
    return RingEOF(**payload)


SERIALIZERS = {
    MsgType.TRANSACTION_BATCH: _serialize_transaction_batch,
    MsgType.EOF: _serialize_eof,
    MsgType.RING_EOF: _serialize_ring_eof,
}

DESERIALIZERS = {
    MsgType.TRANSACTION_BATCH: _deserialize_transaction_batch,
    MsgType.EOF: _deserialize_eof,
    MsgType.RING_EOF: _deserialize_ring_eof,
}
