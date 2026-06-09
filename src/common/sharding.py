import hashlib


def hash_of(key):
    """Stable integer hash of a key (md5 as int). Same key → same value, always."""
    return int(hashlib.md5(str(key).encode()).hexdigest(), 16)


def shard_of(key, num_shards):
    """Stable shard index in [0, num_shards) for the given routing key."""
    return hash_of(key) % num_shards
