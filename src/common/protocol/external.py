from common.models.raw_transaction import RawTransaction
from common.models.raw_account import RawAccount
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
    ACCOUNT_BATCH = 2  # [1byte type] [4 bytes payload_size] [4 bytes count] [account * count] (client -> gateway)
    EOF_TRANSACTIONS = 3  # [1byte type] (client -> gateway)
    EOF_ACCOUNTS = 4  # [1byte type] (client -> gateway)
    QUERY_RESULT = 5  # [1byte type] [4 bytes payload_size] [1byte query_id] [4 bytes count] [result_record * count] (gateway -> client) result_record cambia según query_id porque cada query devuelve algo distinto
    QUERY_END = 6  # [1byte type] [1byte query_id] (gateway -> client)
    REDIRECT = 7  # [1byte type] [4 bytes host_len] [host] [4 bytes port] (proxy -> client)
    ACK = 8  # [1byte type] (gateway -> client)


# TRANSACTION = [4B + N raw_line]
# ACCOUNT = [4B + N raw_line]


# ---------- serializer / deserializer para transactions y accounts ----------


def _serialize_lp_string(s):
    """Serializa un string agregandole su longitud"""
    encoded = external_serializer.serialize_string(s)
    return external_serializer.serialize_uint32(len(encoded)) + encoded


def _deserialize_lp_string(buf, offset):
    """Lee un string length-prefixed. Devuelve (string, nuevo_offset)."""
    length = external_serializer.deserialize_uint32(
        buf[offset : offset + external_serializer.UINT32_SIZE]
    )
    offset += external_serializer.UINT32_SIZE
    s = external_serializer.deserialize_string(buf[offset : offset + length])
    offset += length
    return s, offset


def _serialize_transaction(tx):
    return _serialize_lp_string(tx.raw)


def _deserialize_transaction(buf, offset):
    """Devuelve (RawTransaction, nuevo_offset)."""
    raw, offset = _deserialize_lp_string(buf, offset)
    return RawTransaction(raw=raw), offset


def _serialize_account(account):
    return _serialize_lp_string(account.raw)


def _deserialize_account(buf, offset):
    """Devuelve (RawAccount, nuevo_offset)."""
    raw, offset = _deserialize_lp_string(buf, offset)
    return RawAccount(raw=raw), offset


# ---------- result_record por query ----------


def _serialize_result_record_q1(record):
    """result_record = [4B from_bank_len][N][4B from_account_len][N][4B to_bank_len][N][4B to_account_len][N][8B amount_paid]"""
    return b"".join(
        [
            _serialize_lp_string(record.from_bank),
            _serialize_lp_string(record.from_account),
            _serialize_lp_string(record.to_bank),
            _serialize_lp_string(record.to_account),
            external_serializer.serialize_float64(record.amount_paid),
        ]
    )


def _deserialize_result_record_q1(buf, offset):
    from_bank, offset = _deserialize_lp_string(buf, offset)
    from_account, offset = _deserialize_lp_string(buf, offset)
    to_bank, offset = _deserialize_lp_string(buf, offset)
    to_account, offset = _deserialize_lp_string(buf, offset)
    amount_paid = external_serializer.deserialize_float64(
        buf[offset : offset + external_serializer.FLOAT64_SIZE]
    )
    offset += external_serializer.FLOAT64_SIZE
    return Q1Result(from_bank, from_account, to_bank, to_account, amount_paid), offset


def _serialize_result_record_q2(record):
    """result_record = [4B bank_name_len][N][4B from_account_len][N][8B amount_paid]"""
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
    """result_record = [4B from_bank_len][N][4B from_account_len][N][8B amount_paid]"""
    return b"".join(
        [
            _serialize_lp_string(record.from_bank),
            _serialize_lp_string(record.from_account),
            external_serializer.serialize_float64(record.amount_paid),
        ]
    )


def _deserialize_result_record_q3(buf, offset):
    from_bank, offset = _deserialize_lp_string(buf, offset)
    from_account, offset = _deserialize_lp_string(buf, offset)
    amount_paid = external_serializer.deserialize_float64(
        buf[offset : offset + external_serializer.FLOAT64_SIZE]
    )
    offset += external_serializer.FLOAT64_SIZE
    return Q3Result(from_bank, from_account, amount_paid), offset


def _serialize_result_record_q4(record):
    """result_record = [4B bank_len][N][4B account_len][N]"""
    return b"".join(
        [
            _serialize_lp_string(record.bank),
            _serialize_lp_string(record.account),
        ]
    )


def _deserialize_result_record_q4(buf, offset):
    bank, offset = _deserialize_lp_string(buf, offset)
    account, offset = _deserialize_lp_string(buf, offset)
    return Q4Result(bank, account), offset


def _serialize_result_record_q5(record):
    """result_record = [8B count]"""
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


def _send_batch(sock, msg_type, records, serializer):
    payload = external_serializer.serialize_uint32(len(records))
    for record in records:
        payload += serializer(record)

    frame = (
        external_serializer.serialize_uint8(msg_type)
        + external_serializer.serialize_uint32(len(payload))
        + payload
    )
    sock.send_all(frame)


def _recv_batch(sock, deserializer):
    size = external_serializer.deserialize_uint32(
        sock.recv_exact(external_serializer.UINT32_SIZE)
    )
    payload = sock.recv_exact(size)

    offset = 0
    count = external_serializer.deserialize_uint32(
        payload[offset : offset + external_serializer.UINT32_SIZE]
    )
    offset += external_serializer.UINT32_SIZE

    items = []
    for _ in range(count):
        item, offset = deserializer(payload, offset)
        items.append(item)
    return items


def _send_transaction_batch(sock, transactions):
    _send_batch(sock, MsgType.TRANSACTION_BATCH, transactions, _serialize_transaction)


def _recv_transaction_batch(sock):
    return _recv_batch(sock, _deserialize_transaction)


def _send_account_batch(sock, accounts):
    _send_batch(sock, MsgType.ACCOUNT_BATCH, accounts, _serialize_account)


def _recv_account_batch(sock):
    return _recv_batch(sock, _deserialize_account)


def _send_eof_transactions(sock):
    sock.send_all(external_serializer.serialize_uint8(MsgType.EOF_TRANSACTIONS))


def _recv_eof_transactions(sock):
    return None


def _send_eof_accounts(sock):
    sock.send_all(external_serializer.serialize_uint8(MsgType.EOF_ACCOUNTS))


def _recv_eof_accounts(sock):
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


def _send_redirect(sock, host, port):
    payload = _serialize_lp_string(host) + external_serializer.serialize_uint32(port)
    frame = external_serializer.serialize_uint8(MsgType.REDIRECT) + payload
    sock.send_all(frame)


def _recv_redirect(sock):
    host_len = external_serializer.deserialize_uint32(
        sock.recv_exact(external_serializer.UINT32_SIZE)
    )
    host = external_serializer.deserialize_string(sock.recv_exact(host_len))
    port = external_serializer.deserialize_uint32(
        sock.recv_exact(external_serializer.UINT32_SIZE)
    )
    return host, port


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

def _send_ack(sock):
    sock.send_all(external_serializer.serialize_uint8(MsgType.ACK))

def _recv_ack(sock):
    return None


SEND_MSG_HANDLERS = {
    MsgType.TRANSACTION_BATCH: _send_transaction_batch,
    MsgType.ACCOUNT_BATCH: _send_account_batch,
    MsgType.EOF_TRANSACTIONS: _send_eof_transactions,
    MsgType.EOF_ACCOUNTS: _send_eof_accounts,
    MsgType.QUERY_RESULT: _send_query_result,
    MsgType.QUERY_END: _send_query_end,
    MsgType.REDIRECT: _send_redirect,
    MsgType.ACK: _send_ack,
}

RECV_MSG_HANDLERS = {
    MsgType.TRANSACTION_BATCH: _recv_transaction_batch,
    MsgType.ACCOUNT_BATCH: _recv_account_batch,
    MsgType.EOF_TRANSACTIONS: _recv_eof_transactions,
    MsgType.EOF_ACCOUNTS: _recv_eof_accounts,
    MsgType.QUERY_RESULT: _recv_query_result,
    MsgType.QUERY_END: _recv_query_end,
    MsgType.REDIRECT: _recv_redirect,
    MsgType.ACK: _recv_ack,
}


# ---------- API ----------


def send_msg(sock, msg_type, *args):
    handler = SEND_MSG_HANDLERS[msg_type]
    handler(sock, *args)


def recv_msg(sock):
    msg_type = external_serializer.deserialize_uint8(
        sock.recv_exact(external_serializer.UINT8_SIZE)
    )
    handler = RECV_MSG_HANDLERS[msg_type]
    return msg_type, handler(sock)
