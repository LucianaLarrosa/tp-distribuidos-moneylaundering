import json
from dataclasses import asdict

from common.models.raw_transaction import RawTransaction


class MsgType:
    TRANSACTION = "transaction"
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


def _serialize_transaction(transaction):
    return asdict(transaction)


def _serialize_eof():
    return None


def _deserialize_transaction(payload):
    return RawTransaction(**payload)


def _deserialize_eof(_):
    return None


SERIALIZERS = {
    MsgType.TRANSACTION: _serialize_transaction,
    MsgType.EOF: _serialize_eof,
}

DESERIALIZERS = {
    MsgType.TRANSACTION: _deserialize_transaction,
    MsgType.EOF: _deserialize_eof,
}
