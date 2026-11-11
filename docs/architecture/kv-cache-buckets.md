# KV Cache Buckets

**Use this when:** you design, implement, or review storage for an LLM KV cache (an inference engine's offloaded attention and state pages) in RustFS, change the lockless cache-bucket path, or need to know which object-store guarantees a cache bucket gives up and what replaces them.

Status: the `kv-cache` bucket engine, the first engine on the [bucket engine framework](bucket-engines.md), implements this design in `crates/kvcache` (store core) and `rustfs/src/kvcache_engine` (routing, batch operations, peers), built with the `bucket-engines` feature. The lockless mode (`RUSTFS_LOCKLESS_CACHE_BUCKETS`, `crates/ecstore/src/bucket/lockless_cache.rs`) is a separate, lighter option that keeps the erasure-coded object layout.

## Why a separate engine

A KV cache stores pages of a model's attention keys and values and its recurrent state, keyed by a hash of the token prefix they encode. Three properties separate it from object storage:

- A key always names the same bytes. Writers of one key race only to store identical content, so there is nothing to serialize.
- Any entry may disappear. A missing, stale, or corrupt entry costs a recompute, never a wrong answer, as long as corruption is detected.
- Entries are numerous, written once, and read in prefix order and in large batches: a prompt's pages are only useful up to the first missing one.

The erasure-coded object layer is built for the opposite workload. Each cache page becomes an object sliced across every drive of an erasure set, so every PUT, GET, HEAD, and DELETE fans out to all nodes, takes namespace locks, reads quorum metadata, and writes per-drive metadata. Under KV-offload load that produced bucket-wide lock contention (fixed by the reader-count widening in `crates/lock/src/fast_lock/state.rs` and by the lockless mode), one quorum metadata read per page lookup, millions of small objects per benchmark hour, and per-object deletes slow enough that cleanup fell behind writes and drives filled. The `kv-cache` engine replaces that layout instead of tuning it.

## Contract

A bucket bound to the `kv-cache` engine:

- Stores each entry whole on one drive of one owner node, with no slicing and no parity. Capacity overhead is the entry header only.
- Lets every client reach every key: any node accepts any request, and data moves once, between the owner and the client.
- Never returns bytes that differ from what was written for that key: every read verifies a per-entry checksum and turns a mismatch into a miss.
- May lose any entry at any time: owner unavailable, drive lost, eviction, or a torn write after a crash. Clients treat every failure as a miss.
- Takes no distributed locks and keeps no cross-node metadata. A write of a key that already exists is a no-op that succeeds.
- Bounds its own disk usage: it evicts before any of its drives reaches the high watermark and refuses writes rather than filling a drive.
- Serves only the operations it declares to the framework. It rejects versioning, Object Lock, quota, replication, lifecycle rules, notifications, and table catalogs, and gives no consistency guarantee for listing. The engine binding is fixed when the bucket is created.

Model weights and every other bucket keep the erasure-coded object layer and its guarantees.

## Placement

- **Targets:** every cache drive of every node is a target. The engine's view of targets is the framework's placement view (ordered nodes from the configured endpoints, local drives).
- **Owner:** rendezvous (highest-random-weight) hashing of the entry key over the targets selects the owning target, and so the owner node. Every node and every client computes the same owner without coordination, and adding or removing a target remaps only that target's share of keys.
- **Replication factor:** 1 by default. With 2, the entry is also written to the second-ranked target on a different node. The two writes are independent and idempotent; there is no chain, no version, and no resync. A missing copy is a miss on that copy only.
- **Failure scope:** a down node or a lost drive turns only the keys it owns into misses. In the erasure layout, by contrast, a cache written with parity below the per-node drive count becomes unavailable as a whole when one node is down.

## Cross-node access

Ownership decides where an entry's bytes live, not who can reach them. The cache is one distributed store:

- **Any client reaches any key.** An inference process on any node can query, read, and write every key, whichever node owns it. An acknowledged write is visible to clients on every node.
- **Any RustFS node accepts any request.** Clients route to owners directly (the fast path) or send a whole batch to any node. A node that receives keys it does not own splits the batch by owner and hands each part to its owner through the framework's internode engine call; it never proxies entry data.
- **Data moves once, owner to client.** Requests carry the client's NIXL agent metadata and buffer descriptors. The owner initiates every transfer: it pulls written values from the client with an RDMA read and pushes read values to the client with an RDMA write, even when the request arrived at another node.
- **Batches spanning owners run in parallel.** A prompt's pages hash to all owners, so one restore draws on every node's drives, memory tier, and NICs at once; the prefix query fans out to the owners concurrently and returns the combined longest prefix.
- **Replicas share reads.** With a replication factor of 2, either copy may serve a read, chosen by availability and load.

## Routing and membership

- The placement view carries a version that changes when targets join, leave, or go offline. Clients cache it and stamp every request with it; an owner rejects requests stamped with an older version, and the client refreshes its view and retries.
- Clients refresh the view by asking any node for the current version (pull if newer) on a timer and after a rejection.
- A target that cannot be reached is skipped for writes and treated as a miss for reads until the view changes. There is no recovery protocol: a returning target serves whatever its segments still hold.

## Storage format

- **Location:** each owner keeps its data under the framework's engine directory on each cache drive, `.rustfs.sys/engines/kv-cache/<bucket-incarnation>/`. The scanner, heal, bucket listing, and the delete-recovery volume walk skip it.
- **Segments:** a segment is one preallocated file (`fallocate`) of fixed size, written append-only by one writer per drive with `O_DIRECT` aligned I/O and sealed when full or old. A background allocator keeps a small number of empty segments ready on each drive so allocation never waits on the filesystem.
- **Entries:** each entry is a header (format version, key, value length, value checksum, write time) followed by the value, aligned to the I/O block size so values can be read straight into registered memory. The checksum is CRC32C. A sealed segment ends with a footer that lists its entries.
- **Index:** each owner keeps an in-memory map from key to (drive, segment, offset, length) for the keys it holds, plus per-segment age and live-byte counts. At startup it rebuilds the map from sealed-segment footers and by scanning the open segment's headers; entries that fail their checksum are dropped. There is no persistent metadata store and no write-ahead log.
- **Write-if-absent:** a write of a key already in the index returns success without writing. Concurrent writers of a new key may both append; the index keeps the first complete entry and the other copy is reclaimed with its segment.
- **Durability:** no fsync per entry by default. A crash loses unsynced data in open segments, which the index rebuild discards as torn entries.
- **Readers and eviction:** segments are reference counted. An in-flight read holds its segment, so eviction can unlink a segment at any time without racing an RDMA transfer that is still reading from it; the space is returned when the last reader finishes.

## Data path

- The owner's disk reads and writes go through io_uring with fixed buffers drawn from the same pre-registered memory pool that NIXL uses, so a read moves from NVMe into registered memory and then to the client with an RDMA write, with no intermediate copy. Writes move the other way: RDMA read from the client into the pool, then a direct write to the open segment.
- Each owner limits concurrent RDMA transfers per NIC, and a client limits how many owners may push into it at once, so many owners answering one client cannot overrun that client's NIC.
- Clients group a batch by owner, cap each request by operation count and bytes, and keep a bounded number of requests in flight per owner and in total. Writes are batched like reads.

## Request surface

Batch operations are custom operations of the engine. The framework authorizes each request once with SigV4 and the bucket policy action S3 would require (`s3:GetObject` for queries and reads, `s3:PutObject` for writes) before calling the engine.

- **Prefix query:** ordered entry keys in, the length of the longest stored prefix and each entry's location out. One round trip per owner replaces one metadata lookup per page.
- **Batch read:** (key, destination buffer, length) tuples in; owners push the values into the destination GPU or host memory.
- **Batch write:** (key, source buffer, length) tuples in; owners pull and append the values and acknowledge once each entry is readable from its owner.
- **S3 operations:** GetObject, HeadObject, PutObject, and DeleteObject on single keys stay available for debugging and tooling, served by the owner. ListObjects (v1 and v2, with or without a delimiter) is best effort over the owners' indexes: every node lists its own keys, rolling keys up to common prefixes in the same pass, and the pages are merged, so a key written during a listing may be missing from it.

## Memory tier

Each owner may keep a bounded set of recently written and recently read entries in RDMA-registered memory and serve batch reads from it without disk I/O. The tier caches segment contents; it is never the only copy. Dropping an entry from memory loses nothing, and evicting a segment removes its entries from memory too.

## Eviction and deletion

Deletion is bulk and local by construction, so it cannot fall behind writes the way per-object deletes do.

- **Unit of eviction:** a sealed segment. Dropping it removes its keys from the index and unlinks one file. No locks, no quorum, no delete records, and no other node is involved.
- **Capacity:** every few seconds each owner reads `statvfs` for each cache drive, so data that does not belong to RustFS counts too. Above the high watermark it drops its oldest segments on that drive until usage falls below the low watermark. Above the emergency watermark it refuses new entries on that drive with a distinct cache-full error that clients treat as a skipped write.
- **Age:** segments older than the bucket's maximum age are dropped even when space remains, so stale model or prompt versions do not linger.
- **Hot prefixes survive:** a read that hits an entry in an old segment may rewrite it into the open segment (promotion), so prefixes in use move forward and only cold prefixes age out. Prompt heads shared by many conversations are the last to go, and a dropped segment removes prefix tails before the heads they depend on.
- **Namespaces:** per-model or per-tenant key prefixes may carry their own share of capacity; the evictor drops the over-share prefix's oldest segments first, which requires those prefixes to write to their own segments.
- **Explicit deletes:** deleting a key removes it from the index; its bytes are reclaimed with its segment. Deleting a key prefix does the same for every matching index entry. Purging the bucket removes the engine directories for the bucket incarnation on every node.
- **Bytes per entry:** clients should store recurrent-state snapshots only at the boundaries they restore from. Snapshots dominate the bytes of hybrid attention and state-space models, so this matters more than the eviction policy.

## Failure behavior

| Event | Effect |
|---|---|
| Owner node down | Its keys miss and writes for them are skipped. The rest of the cache is unaffected. |
| Drive lost or unreadable | Entries on it miss; the index drops them at the next rebuild. |
| Drive above the emergency watermark | New entries on it are refused as cache full; reads continue. |
| Crash during a write | Unsynced and torn entries in open segments are discarded at index rebuild. |
| Checksum mismatch on read | The entry is reported as a miss and removed from the index. |
| Placement view changes | Requests with the old version are rejected and retried; keys owned by a changed target miss until rewritten. |

## Configuration

The engine config lives in the bucket's engine entry ([bucket-engines.md](bucket-engines.md)). Its fields are the replication factor, the cache drives to use, segment size and maximum age, the number of preallocated segments per drive, the memory-tier budget, the high, low, and emergency watermarks, per-prefix capacity shares, and the transfer concurrency limits.

## Lessons from 3FS

DeepSeek's 3FS also serves KV caches from SSDs over RDMA, as ordinary files on a general-purpose distributed file system. Its design informed this engine as follows.

| Adopted | How it appears here |
|---|---|
| Preallocated extents that are released in bulk | Fixed-size segment files created ahead of need by a background allocator and dropped whole |
| Reference-counted data positions | Segment reference counts let eviction run while RDMA reads are in flight |
| One registered buffer pool for disk I/O and RDMA | io_uring fixed buffers from the NIXL pool, so NVMe data reaches the client with no copy |
| Server-initiated RDMA in both directions, with per-NIC limits and receiver-side admission control | Owners pull writes and push reads; owner and client transfer limits |
| Client batches grouped by node with operation and byte caps | The same, applied to writes as well as reads |
| Versioned routing information with server-side rejection of stale requests | The versioned placement view |
| Per-disk `statvfs` checks that stop allocation near full | Capacity watermarks that evict first and refuse writes only at the emergency mark |
| Write-if-absent chunk creation | Writes of an existing key succeed without writing |
| Expiry by time bucket and bulk removal instead of per-file deletes | Segment-level eviction by age and capacity |

| Not needed for an immutable, losable cache | Why |
|---|---|
| Chain replication with pending and committed versions, chunk locks, and version checks | Entries never change; replicas are independent idempotent writes |
| Resync, target state machine, and recovery read balancing | A lost or stale copy is a miss; liveness and rehashing are enough |
| Copy-on-write, partial writes, truncation, and compaction by copying | Entries are written whole once; eviction drops whole segments |
| A transactional metadata store and a synced write-ahead log | The index is rebuilt from segment footers; a crash only loses cache entries |
| A global namespace with directories and file-length tracking | Keys are content hashes with no hierarchy |

## Relationship to the object layer

- Bucket metadata (existence, policy, engine entry) stays in `.rustfs.sys` and goes through the normal bucket metadata paths; only entry data and the index belong to the engine.
- The engine shares the server's drives, NIXL agents, and registered memory with the object layer, and its watermarks account for the object layer's usage of the same filesystems.
- The lockless mode is the step before this engine: it keeps the erasure-coded layout but removes the locks and per-request metadata reads that a cache does not need. A bucket uses one or the other.
