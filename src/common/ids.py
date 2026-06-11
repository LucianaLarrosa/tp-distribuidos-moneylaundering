SEPARATOR = ":"
RING_PHASE_COUNT = "count"
RING_PHASE_FLUSH = "flush"


def root_id(client_id, gateway_id, batch_index):
    """Gateway root id for the n-th batch of a (client, gateway) stream."""
    return SEPARATOR.join((str(client_id), str(gateway_id), str(batch_index)))


def flush_id(origin, client_id, gateway_id, n_batch):
    """
    Flush id: restart the chain for a stateful node that merges many inputs.
    `origin` is the producing replica's stable node_id; `n_batch` is the
    content-derived bucket within the destination shard. The shard itself rides
    in the routing key, not the id (each consumer only sees its own shard).
    """
    return SEPARATOR.join((str(client_id), str(gateway_id), str(origin), str(n_batch)))


def ring_id(client_id, gateway_id, phase, seq):
    """Ring EOF token id: phase + per-hop sequence (seq resets at the count->flush boundary)."""
    return SEPARATOR.join((str(client_id), str(gateway_id), str(phase), str(seq)))


def ring_seq_of(message_id):
    """Sequence carried in a ring id, or -1 if the id has none."""
    parts = message_id.split(SEPARATOR)
    if len(parts) < 4:
        return -1
    try:
        return int(parts[-1])
    except ValueError:
        return -1


def eof_id(client_id, gateway_id):
    """Coordinator-independent id for a final EOF / QUERY_END."""
    return SEPARATOR.join((str(client_id), str(gateway_id), "eof"))
