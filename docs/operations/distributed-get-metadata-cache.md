# Distributed GetObject metadata cache

`RUSTFS_GET_OBJECT_METADATA_CACHE_DISTRIBUTED_ENABLE` opts a distributed erasure cluster into the per-process GetObject metadata cache. It defaults to `false`; the value is read at process start, so changing it requires restarting the affected RustFS process.

Every node must run a build that implements protocol v2 and use the same value. Before a process first uses the cache or commits a mutation, it probes the topology peers through the authenticated metadata-cache RPC. A missing, unreachable, incompatible, or differently configured peer keeps that process on the uncached path and rejects mutations before object commit. This is intentional fail-closed behavior; restore one uniform cluster configuration before retrying. The value is read at process start, so a coordinated restart is required to change it.

Object mutations begin a peer fence before changing authoritative metadata. Recursive prefix deletion uses an all-cache scope because it can remove an unbounded set of object keys. Peers invalidate their full metadata cache and bypass cache lookup/publication while that scope is pending. The coordinator releases the fence only after delete quorum succeeds. If the outcome is uncertain, pending state remains fail-closed until a matching terminal phase is replayed or the affected process restarts with an empty in-memory cache.

Do not roll back to, or rejoin, a peer running an older protocol while the cache is enabled: an older process cannot participate in the mutation fence. Disable the cache across the cluster before a version rollback or mixed-version recovery, and keep it disabled until all peers run protocol v2 again.

The cache is an in-memory optimization. Its presence does not change object data durability, the storage read/write quorum, versioning semantics, or client-visible S3 metadata.
