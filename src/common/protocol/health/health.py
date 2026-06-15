from . import health_pb2 as pb


def serialize_ping():
    return pb.Ping().SerializeToString()


def deserialize_ping(data):
    pb.Ping().ParseFromString(data)


def serialize_pong(node_name):
    return pb.Pong(node_name=node_name).SerializeToString()


def deserialize_pong(data):
    pong = pb.Pong()
    pong.ParseFromString(data)
    return pong.node_name
