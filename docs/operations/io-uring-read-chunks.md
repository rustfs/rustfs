# io_uring Read Chunk Size

**Use this when:** tuning the maximum logical size of one io_uring read operation without changing descriptor caching, shard count or queue depth.
**Source of truth:** `crates/ecstore/src/disk/local.rs` (`UringBackend`, `pread_uring`, `pread_uring_direct`) and `crates/ecstore/src/disk/uring_read_chunks.rs` (`ReadChunkSize`).

## Configuration

`RUSTFS_IO_URING_READ_CHUNK_BYTES` is an unsigned decimal byte count from `4096`
through `134217728` (128 MiB), inclusive. When unset, the existing 128 MiB cap
is retained. Empty, signed, whitespace-padded, non-Unicode, zero, overflowing
and out-of-range values are rejected: io_uring backend construction falls back
to std with one warning that does not print the raw value.

The setting is read once on first io_uring construction and requires a process
restart to change. It does not enable io_uring by itself. Disabled io_uring and
non-Linux production paths continue to use the standard backend.

## Read behavior

Each backend retains a validated cap. Both buffered and direct positioned reads
use one driver operation for a logical range no larger than that cap; larger
ranges use sequential chunks and assemble the complete result. Zero-length
reads preserve their existing validation and result semantics.

The cap need not be a power of two or a device-block multiple: `4097` is valid.
For direct reads, the existing driver aligns each chunk's enclosing physical
range and returns only its logical bytes. Adjacent operations can reread boundary
blocks. Length/EOF checks, error classification, fallback and page-cache reclaim
remain in their existing paths; a short chunk is not accepted as a complete
object-range result.

Smaller chunks trade lower logical bytes per submitted operation for more
submissions, wakeups and result assembly. The default is unchanged and there is
no measured performance claim. Compare matched workloads before adopting a
smaller deployment value.

## Not a byte quota

This is a per-operation logical length cap, not a weighted admission budget.
Direct alignment head/tail and allocation padding can exceed it. Concurrent
operations across rings and disks multiply allocation, and canceled operations
may remain owned by the driver until completion.

The full assembly allocation, completed/retained results, allocator overhead,
kernel resources and std fallback allocations are outside this cap. Changing
chunk size therefore does not enforce process RSS, total request memory or an
end-to-end read-buffer quota, and does not replace a driver-owned byte lease.
