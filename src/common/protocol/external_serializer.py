import struct

UINT8_SIZE = 1
UINT32_SIZE = 4
UINT64_SIZE = 8
FLOAT64_SIZE = 8


def serialize_uint8(u):
    return u.to_bytes(UINT8_SIZE, "big")


def deserialize_uint8(b):
    return int.from_bytes(b, byteorder="big", signed=False)


def serialize_uint32(u):
    return u.to_bytes(UINT32_SIZE, "big")


def deserialize_uint32(b):
    return int.from_bytes(b, byteorder="big", signed=False)


def serialize_uint64(u):
    return u.to_bytes(UINT64_SIZE, "big")


def deserialize_uint64(b):
    return int.from_bytes(b, byteorder="big", signed=False)


def serialize_float64(f):
    return struct.pack("!d", f)


def deserialize_float64(b):
    return struct.unpack("!d", b)[0]


def deserialize_string(b):
    return b.decode("utf-8")


def serialize_string(s):
    return s.encode("utf-8")
