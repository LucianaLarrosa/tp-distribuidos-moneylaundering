SEPARATOR = ":"


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
