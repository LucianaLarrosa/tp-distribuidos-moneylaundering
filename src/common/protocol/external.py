from datetime import datetime

from common.models.transaction import Transaction
from common.models.query_results import (
    Q1Result,
    Q2Result,
    Q3Result,
    Q4Result,
    Q5Result,
)
from . import external_serializer


class MsgType:
    TRANSACTION_BATCH = 1  # [1byte type] [4 bytes payload_size] [4 bytes count] [transaction * count] (client -> gateway)
    EOF = 2  # [1byte type] (client -> gateway)
    ACK = 3  # [1byte type] (gateway -> client)
    QUERY_RESULT = 4  # [1byte type] [4 bytes payload_size] [1byte query_id] [4 bytes count] [result_record * count] (gateway -> client) result_record cambia según query_id porque cada query devuelve algo distinto
    QUERY_END = 5  # [1byte type] [1byte query_id] (gateway -> client)


# QUERY 1: result_record = [4B from_bank][4B from_account_len][N][4B to_bank][4B to_account_len][N][8B amount_paid]
# QUERY 2: result_record = [4B bank_name_len][N][4B from_account_len][N][8B amount_paid]
# QUERY 3: result_record = [4B from_bank][4B from_account_len][N][8B amount_paid]
# QUERY 4: result_record = [4B from_bank][4B from_account_len][N][4B to_bank][4B to_account_len][N]
# QUERY 5: result_record = [8B count]
#
# TRANSACTION = [8B timestamp_ms][4B from_bank][4B+N from_account][4B to_bank][4B+N to_account]
#               [8B amount_received][4B+N receiving_currency][8B amount_paid][4B+N payment_currency]
#               [4B+N payment_format][1B is_laundering]


def _serialize_lp_string(s):
    """Serializa un string agregandole su longitud"""
    encoded = external_serializer.serialize_string(s)
    return external_serializer.serialize_uint32(len(encoded)) + encoded


def _serialize_transaction(tx):
    timestamp_ms = int(tx.timestamp.timestamp() * 1000)
    return b"".join(
        [
            external_serializer.serialize_uint64(timestamp_ms),
            external_serializer.serialize_uint32(tx.from_bank),
            _serialize_lp_string(tx.from_account),
            external_serializer.serialize_uint32(tx.to_bank),
            _serialize_lp_string(tx.to_account),
            external_serializer.serialize_float64(tx.amount_received),
            _serialize_lp_string(tx.receiving_currency),
            external_serializer.serialize_float64(tx.amount_paid),
            _serialize_lp_string(tx.payment_currency),
            _serialize_lp_string(tx.payment_format),
            external_serializer.serialize_uint8(int(tx.is_laundering)),
        ]
    )


def _deserialize_lp_string(buf, offset):
    """Lee un string length-prefixed. Devuelve (string, nuevo_offset)."""
    length = external_serializer.deserialize_uint32(
        buf[offset : offset + external_serializer.UINT32_SIZE]
    )
    offset += external_serializer.UINT32_SIZE
    s = external_serializer.deserialize_string(buf[offset : offset + length])
    offset += length
    return s, offset


def _deserialize_transaction(buf, offset):
    """Devuelve (Transaction, nuevo_offset)."""
    timestamp_ms = external_serializer.deserialize_uint64(
        buf[offset : offset + external_serializer.UINT64_SIZE]
    )
    offset += external_serializer.UINT64_SIZE

    from_bank = external_serializer.deserialize_uint32(
        buf[offset : offset + external_serializer.UINT32_SIZE]
    )
    offset += external_serializer.UINT32_SIZE

    from_account, offset = _deserialize_lp_string(buf, offset)

    to_bank = external_serializer.deserialize_uint32(
        buf[offset : offset + external_serializer.UINT32_SIZE]
    )
    offset += external_serializer.UINT32_SIZE

    to_account, offset = _deserialize_lp_string(buf, offset)

    amount_received = external_serializer.deserialize_float64(
        buf[offset : offset + external_serializer.FLOAT64_SIZE]
    )
    offset += external_serializer.FLOAT64_SIZE

    receiving_currency, offset = _deserialize_lp_string(buf, offset)

    amount_paid = external_serializer.deserialize_float64(
        buf[offset : offset + external_serializer.FLOAT64_SIZE]
    )
    offset += external_serializer.FLOAT64_SIZE

    payment_currency, offset = _deserialize_lp_string(buf, offset)
    payment_format, offset = _deserialize_lp_string(buf, offset)

    is_laundering = bool(
        external_serializer.deserialize_uint8(
            buf[offset : offset + external_serializer.UINT8_SIZE]
        )
    )
    offset += external_serializer.UINT8_SIZE

    tx = Transaction(
        timestamp=datetime.fromtimestamp(timestamp_ms / 1000),
        from_bank=from_bank,
        from_account=from_account,
        to_bank=to_bank,
        to_account=to_account,
        amount_received=amount_received,
        receiving_currency=receiving_currency,
        amount_paid=amount_paid,
        payment_currency=payment_currency,
        payment_format=payment_format,
        is_laundering=is_laundering,
    )
    return tx, offset


# ---------- result_record por query ----------


def _serialize_result_record_q1(record):
    return b"".join(
        [
            external_serializer.serialize_uint32(record.from_bank),
            _serialize_lp_string(record.from_account),
            external_serializer.serialize_uint32(record.to_bank),
            _serialize_lp_string(record.to_account),
            external_serializer.serialize_float64(record.amount_paid),
        ]
    )


def _deserialize_result_record_q1(buf, offset):
    from_bank = external_serializer.deserialize_uint32(
        buf[offset : offset + external_serializer.UINT32_SIZE]
    )
    offset += external_serializer.UINT32_SIZE
    from_account, offset = _deserialize_lp_string(buf, offset)
    to_bank = external_serializer.deserialize_uint32(
        buf[offset : offset + external_serializer.UINT32_SIZE]
    )
    offset += external_serializer.UINT32_SIZE
    to_account, offset = _deserialize_lp_string(buf, offset)
    amount_paid = external_serializer.deserialize_float64(
        buf[offset : offset + external_serializer.FLOAT64_SIZE]
    )
    offset += external_serializer.FLOAT64_SIZE
    return Q1Result(from_bank, from_account, to_bank, to_account, amount_paid), offset


def _serialize_result_record_q2(record):
    return b"".join(
        [
            _serialize_lp_string(record.bank_name),
            _serialize_lp_string(record.from_account),
            external_serializer.serialize_float64(record.amount_paid),
        ]
    )


def _deserialize_result_record_q2(buf, offset):
    bank_name, offset = _deserialize_lp_string(buf, offset)
    from_account, offset = _deserialize_lp_string(buf, offset)
    amount_paid = external_serializer.deserialize_float64(
        buf[offset : offset + external_serializer.FLOAT64_SIZE]
    )
    offset += external_serializer.FLOAT64_SIZE
    return Q2Result(bank_name, from_account, amount_paid), offset


def _serialize_result_record_q3(record):
    return b"".join(
        [
            external_serializer.serialize_uint32(record.from_bank),
            _serialize_lp_string(record.from_account),
            external_serializer.serialize_float64(record.amount_paid),
        ]
    )


def _deserialize_result_record_q3(buf, offset):
    from_bank = external_serializer.deserialize_uint32(
        buf[offset : offset + external_serializer.UINT32_SIZE]
    )
    offset += external_serializer.UINT32_SIZE
    from_account, offset = _deserialize_lp_string(buf, offset)
    amount_paid = external_serializer.deserialize_float64(
        buf[offset : offset + external_serializer.FLOAT64_SIZE]
    )
    offset += external_serializer.FLOAT64_SIZE
    return Q3Result(from_bank, from_account, amount_paid), offset


def _serialize_result_record_q4(record):
    return b"".join(
        [
            external_serializer.serialize_uint32(record.from_bank),
            _serialize_lp_string(record.from_account),
            external_serializer.serialize_uint32(record.to_bank),
            _serialize_lp_string(record.to_account),
        ]
    )


def _deserialize_result_record_q4(buf, offset):
    from_bank = external_serializer.deserialize_uint32(
        buf[offset : offset + external_serializer.UINT32_SIZE]
    )
    offset += external_serializer.UINT32_SIZE
    from_account, offset = _deserialize_lp_string(buf, offset)
    to_bank = external_serializer.deserialize_uint32(
        buf[offset : offset + external_serializer.UINT32_SIZE]
    )
    offset += external_serializer.UINT32_SIZE
    to_account, offset = _deserialize_lp_string(buf, offset)
    return Q4Result(from_bank, from_account, to_bank, to_account), offset


def _serialize_result_record_q5(record):
    return external_serializer.serialize_uint64(record.count)


def _deserialize_result_record_q5(buf, offset):
    count = external_serializer.deserialize_uint64(
        buf[offset : offset + external_serializer.UINT64_SIZE]
    )
    offset += external_serializer.UINT64_SIZE
    return Q5Result(count), offset


RESULT_RECORD_SERIALIZERS = {
    1: _serialize_result_record_q1,
    2: _serialize_result_record_q2,
    3: _serialize_result_record_q3,
    4: _serialize_result_record_q4,
    5: _serialize_result_record_q5,
}

RESULT_RECORD_DESERIALIZERS = {
    1: _deserialize_result_record_q1,
    2: _deserialize_result_record_q2,
    3: _deserialize_result_record_q3,
    4: _deserialize_result_record_q4,
    5: _deserialize_result_record_q5,
}


# ---------- handlers send / recv por tipo de mensaje ----------


def _send_transaction_batch(sock, transactions):
    payload = external_serializer.serialize_uint32(len(transactions))
    for tx in transactions:
        payload += _serialize_transaction(tx)

    frame = (
        external_serializer.serialize_uint8(MsgType.TRANSACTION_BATCH)
        + external_serializer.serialize_uint32(len(payload))
        + payload
    )
    sock.send_all(frame)


def _recv_transaction_batch(sock):
    size = external_serializer.deserialize_uint32(
        sock.recv_exact(external_serializer.UINT32_SIZE)
    )
    payload = sock.recv_exact(size)

    offset = 0
    count = external_serializer.deserialize_uint32(
        payload[offset : offset + external_serializer.UINT32_SIZE]
    )
    offset += external_serializer.UINT32_SIZE

    transactions = []
    for _ in range(count):
        tx, offset = _deserialize_transaction(payload, offset)
        transactions.append(tx)
    return transactions


def _send_eof(sock):
    sock.send_all(external_serializer.serialize_uint8(MsgType.EOF))


def _recv_eof(sock):
    return None


def _send_ack(sock):
    sock.send_all(external_serializer.serialize_uint8(MsgType.ACK))


def _recv_ack(sock):
    return None


def _send_query_result(sock, query_id, records):
    serializer = RESULT_RECORD_SERIALIZERS[query_id]

    payload = external_serializer.serialize_uint8(query_id)
    payload += external_serializer.serialize_uint32(len(records))
    for record in records:
        payload += serializer(record)

    frame = (
        external_serializer.serialize_uint8(MsgType.QUERY_RESULT)
        + external_serializer.serialize_uint32(len(payload))
        + payload
    )
    sock.send_all(frame)


def _recv_query_result(sock):
    size = external_serializer.deserialize_uint32(
        sock.recv_exact(external_serializer.UINT32_SIZE)
    )
    payload = sock.recv_exact(size)

    offset = 0
    query_id = external_serializer.deserialize_uint8(
        payload[offset : offset + external_serializer.UINT8_SIZE]
    )
    offset += external_serializer.UINT8_SIZE
    count = external_serializer.deserialize_uint32(
        payload[offset : offset + external_serializer.UINT32_SIZE]
    )
    offset += external_serializer.UINT32_SIZE

    deserializer = RESULT_RECORD_DESERIALIZERS[query_id]
    records = []
    for _ in range(count):
        record, offset = deserializer(payload, offset)
        records.append(record)
    return query_id, records


def _send_query_end(sock, query_id):
    frame = external_serializer.serialize_uint8(
        MsgType.QUERY_END
    ) + external_serializer.serialize_uint8(query_id)
    sock.send_all(frame)


def _recv_query_end(sock):
    query_id = external_serializer.deserialize_uint8(
        sock.recv_exact(external_serializer.UINT8_SIZE)
    )
    return query_id


SEND_MSG_HANDLERS = {
    MsgType.TRANSACTION_BATCH: _send_transaction_batch,
    MsgType.EOF: _send_eof,
    MsgType.ACK: _send_ack,
    MsgType.QUERY_RESULT: _send_query_result,
    MsgType.QUERY_END: _send_query_end,
}

RECV_MSG_HANDLERS = {
    MsgType.TRANSACTION_BATCH: _recv_transaction_batch,
    MsgType.EOF: _recv_eof,
    MsgType.ACK: _recv_ack,
    MsgType.QUERY_RESULT: _recv_query_result,
    MsgType.QUERY_END: _recv_query_end,
}


# ---------- API ----------


def send_msg(sock, msg_type, *args):
    handler = SEND_MSG_HANDLERS[msg_type]
    handler(sock, *args)


def recv_msg(sock):
    """Devuelve (msg_type, payload)."""
    msg_type = external_serializer.deserialize_uint8(
        sock.recv_exact(external_serializer.UINT8_SIZE)
    )
    handler = RECV_MSG_HANDLERS[msg_type]
    return msg_type, handler(sock)
