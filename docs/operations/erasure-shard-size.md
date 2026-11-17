# Erasure shard size (`RUSTFS_ERASURE_SHARD_BYTES`)

RustFS writes every object as a sequence of erasure blocks; each block is split
into `data` shards of `ceil(block / data)` bytes and `parity` shards of the
same size. The historical block is 1 MiB (`BLOCK_SIZE_V2`), which on a 12+4
layout gives 87,382-byte shards and about 1,000 stripes per GiB. Every stripe
costs a round of shard reads, a quorum wait, a bitrot check and a reconstruct,
so single-stream GET throughput is bound by per-stripe latency rather than by
disks or the network.

`RUSTFS_ERASURE_SHARD_BYTES` sets the shard size of newly written objects
directly; the block becomes `data shards x shard bytes`, so shards stay exactly
this large and aligned whatever the parity setting:

| Layout | `RUSTFS_ERASURE_SHARD_BYTES=4194304` | Block |
| --- | --- | --- |
| 12+4 (EC:4) | 4 MiB shards | 48 MiB |
| 14+2 (EC:2) | 4 MiB shards | 56 MiB |

Rules:

- `0` or unset keeps the 1 MiB block. The value must be a multiple of 4 KiB and
  at most 64 MiB; anything else is ignored with a startup warning.
- The block is recorded per object in `xl.meta`, so existing objects keep
  their block and keep working. Re-upload objects that should use the new
  layout. Set the same value on every node.
- System metadata under `.rustfs.sys` always uses the 1 MiB block.
- Memory per in-flight stripe grows with the block: about 64 MiB per GET with
  4 MiB shards on 12+4, 128 MiB when a stripe is prefetched. Time to first byte
  of a small ranged read inside a large object grows with the block as well.

## Bitrot verification with large shards

The shard reads of one stripe complete on the decode task, and each frame is
bitrot-hashed as it lands, so with large shards the twelve hashes of a 12+4
stripe used to run one after another on one core (about 10 ms per 48 MiB
stripe). Frames of at least `RUSTFS_BITROT_VERIFY_OFFLOAD_MIN_BYTES`
(default 2 MiB, `0` disables) are hashed on the blocking thread pool
instead, so the shards of a stripe verify in parallel; smaller frames stay
inline where a thread hop would cost more than the hash.
