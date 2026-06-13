UINT32_SIZE = 4


def serialize_uint32(u):
    return u.to_bytes(UINT32_SIZE, "big")


def deserialize_uint32(b):
    return int.from_bytes(b, byteorder="big", signed=False)
