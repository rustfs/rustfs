# Pool metadata upgrade and recovery

`pool.bin` is cluster state. Do not delete or copy it independently on a live
node. Version 3 adds a deployment identity, epoch, durable generation, and a
recoverable prepare/commit record on every pool.

## Compatibility matrix

| Reader or writer | V1 | V2 | V3 |
| --- | --- | --- | --- |
| Legacy V1 binary | read/write | reject | reject |
| V2-capable binary | read/write while mixed | read/write after the V2 fleet gate | reject |
| V3-capable binary | read/migrate | read/migrate | read/write; never downgrade |

Leave `RUSTFS_POOL_META_V3_WRITE` or
`RUSTFS_POOL_META_V3_FLEET_CONFIRMED` disabled while any running process lacks
V3 support. Both must be `true` before an existing cluster migrates. A fresh
deployment can initialize directly at V3. Once a committed V3 generation is
observed, rollback to a V1/V2-only binary is not supported.
Repairing a missing identity on an existing V1/V2 snapshot does not cross the
V3 gate; the identity is committed as initialized while `pool.bin` stays on its
observed legacy version.

Unknown fields are not ignored. An unsupported version or field layout is
reported as **incompatible** and is never overwritten. A truncated or invalid
payload is **corrupt** and may be repaired only from a verified committed
replica. Conflicting identities, epochs, or transactions at the same generation
are **recovery required** and need an operator-selected source.

## Partial writes

A V3 update first conditionally writes a pending generation containing the last
committed snapshot, then conditionally replaces it with the committed record.
During initial bootstrap, `pool.bin.identity` remains `initialized=false` and
carries a unique fresh-bootstrap nonce until that committed V3 record is
verified. Restarting from an initial prepare record finishes generation 1; it
never rewrites the record as V1 or V2.
On restart:

- prepare-only replicas expose their previous committed snapshot;
- one committed replica makes that transaction authoritative;
- remaining pending or older replicas are repairable by the next fenced save;
- two different committed transactions at one generation stop startup.

Do not hand-edit a pending record or select a replica only because it is in pool
zero. Preserve all copies when escalating recovery.

## Disk replacement and metadata erasure

1. Keep a quorum of nodes online and verify the cluster is ready.
2. Stop the lagging node before replacing or erasing its metadata drive.
3. Restore storage formats and the `pool.bin.identity` marker from the same
   deployment before rejoining it.
4. Start the node and wait for it to load the verified committed generation and
   repair its replicas before touching another node.

An initialized identity with every `pool.bin` missing is recovery required.
Existing storage formats with neither identity nor `pool.bin` are also recovery
required. Format creation alone is not fresh-cluster proof. Only the elected
first topology node may create a durable `initialized=false` bootstrap identity
with a fresh-bootstrap nonce, and only after every configured disk explicitly
responds that it is unformatted.
An unreachable peer, a non-elected distributed node, or an existing format is
not sufficient proof. All-missing `pool.bin` replicas are accepted only by the
same startup that proved the fresh topology and persisted that pending identity.
When every `pool.bin` is missing, a later startup must recover even if the
pending identity survived. This prevents a wiped or lagging node from rebuilding
empty state and overwriting the cluster. Runtime reload, rebalance activation,
and rebalance worker admission all fail closed and latch the same recovery gate
until the node is restarted with readable metadata.
