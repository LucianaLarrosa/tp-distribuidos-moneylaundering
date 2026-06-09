from . import health_pb2 as pb


def serialize_heartbeat(node_name):
    return pb.Heartbeat(node_name=node_name).SerializeToString()


def deserialize_heartbeat(data):
    heartbeat = pb.Heartbeat()
    heartbeat.ParseFromString(data)
    return heartbeat.node_name
