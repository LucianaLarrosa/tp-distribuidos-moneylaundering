import json
from dataclasses import asdict

from common.models.raw_transaction import RawTransaction


class MsgType:
    TRANSACTION_BATCH = "transaction_batch"
    EOF = "eof"


# ---------- API ----------


def serialize_msg(msg_type, client_id, gateway_id, *args):
    handler = SERIALIZERS[msg_type]
    payload = handler(*args)
    return json.dumps({
        "type": msg_type,
        "client_id": client_id,
        "gateway_id": gateway_id,
        "payload": payload,
    }).encode("utf-8")


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
    tx_serialized = []
    for tx in transactions:
        tx_serialized.append(asdict(tx))
    return tx_serialized


def _serialize_eof():
    return None


def _deserialize_transaction_batch(payload):
    transactions = []
    for tx in payload:
        transactions.append(RawTransaction(**tx))
    return transactions


def _deserialize_eof(_):
    return None


SERIALIZERS = {
    MsgType.TRANSACTION_BATCH: _serialize_transaction_batch,
    MsgType.EOF: _serialize_eof,
}

DESERIALIZERS = {
    MsgType.TRANSACTION_BATCH: _deserialize_transaction_batch,
    MsgType.EOF: _deserialize_eof,
}
