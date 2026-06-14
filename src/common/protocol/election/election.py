from common.protocol.length_prefix import (
    UINT32_SIZE,
    deserialize_uint32,
    serialize_uint32,
)

from . import election_pb2 as pb


class MsgType:
    ELECTION = 1
    ANSWER = 2
    COORDINATOR = 3


TYPES = {
    "election": MsgType.ELECTION,
    "answer": MsgType.ANSWER,
    "coordinator": MsgType.COORDINATOR,
}


def send_peer_id(sock, peer_id):
    sock.send(serialize_uint32(peer_id))


def recv_peer_id(sock):
    return deserialize_uint32(sock.recv(UINT32_SIZE))


def send_msg(sock, data):
    sock.send(serialize_uint32(len(data)) + data)


def recv_msg(sock, timeout=None):
    size = deserialize_uint32(sock.recv(UINT32_SIZE, timeout=timeout))
    return deserialize_msg(sock.recv(size))


def serialize_election(sender_id):
    return pb.ElectionMessage(
        election=pb.Election(sender_id=sender_id)
    ).SerializeToString()


def serialize_answer(sender_id):
    return pb.ElectionMessage(answer=pb.Answer(sender_id=sender_id)).SerializeToString()


def serialize_coordinator(leader_id):
    return pb.ElectionMessage(
        coordinator=pb.Coordinator(leader_id=leader_id)
    ).SerializeToString()


def deserialize_msg(data):
    msg = pb.ElectionMessage()
    msg.ParseFromString(data)
    field = msg.WhichOneof("msg")
    msg_type = TYPES[field]
    payload = getattr(msg, field)
    node_id = (
        payload.leader_id if msg_type == MsgType.COORDINATOR else payload.sender_id
    )
    return msg_type, node_id
