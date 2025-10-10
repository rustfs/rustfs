# Append Write Design

This document captures the current design of the append-write feature in RustFS so that new contributors can quickly understand the moving parts, data flows, and testing expectations.

## Goals & Non-Goals

### Goals
- Allow clients to append payloads to existing objects without re-uploading the full body.
- Support inline objects and spill seamlessly into segmented layout once thresholds are exceeded.
- Preserve strong read-after-write semantics via optimistic concurrency controls (ETag / epoch).
- Expose minimal S3-compatible surface area (`x-amz-object-append`, `x-amz-append-position`, `x-amz-append-action`).

### Non-Goals
- Full multipart-upload parity; append is intentionally simpler and serialized per object.
- Cross-object transactions; each object is isolated.
- Rebalancing or background compaction (future work).

## State Machine

Append state is persisted inside `FileInfo.metadata` under `x-rustfs-internal-append-state` and serialized as `AppendState` (`crates/filemeta/src/append.rs`).

```
Disabled --(initial PUT w/o append)--> SegmentedSealed
Inline --(inline append)--> Inline / InlinePendingSpill
InlinePendingSpill --(spill success)--> SegmentedActive
SegmentedActive --(Complete)--> SegmentedSealed
SegmentedActive --(Abort)--> SegmentedSealed
SegmentedSealed --(new append)--> SegmentedActive
```

Definitions:
- **Inline**: Object data fully stored in metadata (`FileInfo.data`).
- **InlinePendingSpill**: Inline data after append exceeded inline threshold; awaiting spill to disk.
- **SegmentedActive**: Object data lives in erasure-coded part(s) plus one or more pending append segments on disk (`append/<epoch>/<uuid>`).
- **SegmentedSealed**: No pending segments; logical content equals committed parts.

`AppendState` fields:
- `state`: current state enum (see above).
- `epoch`: monotonically increasing counter for concurrency control.
- `committed_length`: logical size already durable in the base parts/inline region.
- `pending_segments`: ordered list of `AppendSegment { offset, length, data_dir, etag, epoch }`.

## Metadata & Storage Layout

### Inline Objects
- Inline payload stored in `FileInfo.data`.
- Hash metadata maintained through `append_inline_data` (re-encoding with bitrot writer when checksums exist).
- When spilling is required, inline data is decoded, appended, and re-encoded into erasure shards written to per-disk `append/<epoch>/<segment_id>/part.1` temporary path before rename to primary data directory.

### Segmented Objects
- Base object content is represented by standard erasure-coded parts (`FileInfo.parts`, `FileInfo.data_dir`).
- Pending append segments live under `<object>/append/<epoch>/<segment_uuid>/part.1` (per disk).
- Each append stores segment metadata (`etag`, `offset`, `length`) inside `AppendState.pending_segments` and updates `FileInfo.size` to include pending bytes.
- Aggregate ETag is recomputed using multipart MD5 helper (`get_complete_multipart_md5`).

### Metadata Writes
- `SetDisks::write_unique_file_info` persists `FileInfo` updates to the quorum of disks.
- During spill/append/complete/abort, all mirrored `FileInfo` copies within `parts_metadata` are updated to keep nodes consistent.
- Abort ensures inline markers are cleared (`x-rustfs-internal-inline-data`) and `FileInfo.data = None` to avoid stale inline reads.

## Request Flows

### Append (Inline Path)
1. Handler (`rustfs/src/storage/ecfs.rs`) validates headers and fills `ObjectOptions.append_*`.
2. `SetDisks::append_inline_object` verifies append position using `AppendState` snapshot.
3. Existing inline payload decoded (if checksums present) and appended in-memory (`append_inline_data`).
4. Storage class decision determines whether to remain inline or spill.
5. Inline success updates `FileInfo.data`, metadata, `AppendState` (state `Inline`, lengths updated).
6. Spill path delegates to `spill_inline_into_segmented` (see segmented path below).

### Append (Segmented Path)
1. `SetDisks::append_segmented_object` validates state (must be `SegmentedActive` or `SegmentedSealed`).
2. Snapshot expected offset = committed length + sum of pending segments.
3. Payload encoded using erasure coding; shards written to temp volume; renamed into `append/<epoch>/<segment_uuid>` under object data directory.
4. New `AppendSegment` pushed, `AppendState.epoch` incremented, aggregated ETag recalculated.
5. `FileInfo.size` reflects committed + pending bytes; metadata persisted across quorum.

### GET / Range Reads
1. `SetDisks::get_object_with_fileinfo` inspects `AppendState`.
2. Reads committed data from inline or erasure parts (ignoring inline buffers once segmented).
3. If requested range includes pending segments, loader fetches each segment via `load_pending_segment`, decodes shards, and streams appended bytes.

### Complete Append (`x-amz-append-action: complete`)
1. `complete_append_object` fetches current `FileInfo`, ensures pending segments exist.
2. Entire logical object (committed + pending) streamed through `VecAsyncWriter` (TODO: potential optimization) to produce contiguous payload.
3. Inline spill routine (`spill_inline_into_segmented`) consolidates data into primary part, sets state `SegmentedSealed`, clears pending list, updates `committed_length`.
4. Pending segment directories removed and quorum metadata persisted.

### Abort Append (`x-amz-append-action: abort`)
1. `abort_append_object` removes pending segment directories.
2. Ensures `committed_length` matches actual durable data (inline length or sum of parts); logs and corrects if mismatch is found.
3. Clears pending list, sets state `SegmentedSealed`, bumps epoch, removes inline markers/data.
4. Persists metadata and returns base ETag (multipart MD5 of committed parts).

## Error Handling & Recovery

- All disk writes go through quorum helpers (`reduce_write_quorum_errs`, `reduce_read_quorum_errs`) and propagate `StorageError` variants for HTTP mapping.
- Append operations are single-threaded per object via locking in higher layers (`fast_lock_manager` in `SetDisks::put_object`).
- On spill/append rename failure, temp directories are cleaned up; operation aborts without mutating metadata.
- Abort path now realigns `committed_length` if metadata drifted (observed during development) and strips inline remnants to prevent stale reads.
- Pending segments are only removed once metadata update succeeds; no partial deletion is performed ahead of state persistence.

## Concurrency

- Append requests rely on exact `x-amz-append-position` to ensure the client has an up-to-date view.
- Optional header `If-Match` is honored in S3 handler before actual append (shared with regular PUT path).
- `AppendState.epoch` increments after each append/complete/abort; future work may expose it for stronger optimistic control.
- e2e test `append_segments_concurrency_then_complete` verifies that simultaneous appends result in exactly one success; the loser receives 400.

## Key Modules

- `crates/ecstore/src/set_disk.rs`: core implementation (inline append, spill, segmented append, complete, abort, GET integration).
- `crates/ecstore/src/erasure_coding/{encode,decode}.rs`: encode/decode helpers used by append pipeline.
- `crates/filemeta/src/append.rs`: metadata schema + helper functions.
- `rustfs/src/storage/ecfs.rs`: HTTP/S3 layer that parses headers and routes to append operations.

## Testing Strategy

### Unit Tests
- `crates/filemeta/src/append.rs` covers serialization and state transitions.
- `crates/ecstore/src/set_disk.rs` contains lower-level utilities and regression tests for metadata helpers.
- Additional unit coverage is recommended for spill/append failure paths (e.g., injected rename failures).

### End-to-End Tests (`cargo test --package e2e_test append`)
- Inline append success, wrong position, precondition failures.
- Segmented append success, wrong position, wrong ETag.
- Spill threshold transition (`append_threshold_crossing_inline_to_segmented`).
- Pending segment streaming (`append_range_requests_across_segments`).
- Complete append consolidates pending segments.
- Abort append discards pending data and allows new append.
- Concurrency: two clients racing to append, followed by additional append + complete.

### Tooling Considerations
- `make clippy` must pass; the append code relies on async operations and custom logging.
- `make test` / `cargo nextest run` recommended before submitting PRs.
- Use `RUST_LOG=rustfs_ecstore=debug` when debugging append flows; targeted `info!`/`warn!` logs are emitted during spill/abort.

## Future Work

- Streamed consolidation in `complete_append_object` to avoid buffering entire logical object.
- Throttling or automatic `Complete` when pending segments exceed size/quantity thresholds.
- Stronger epoch exposure to clients (header-based conflict detection).
- Automated cleanup or garbage collection for orphaned `append/*` directories.

---

For questions or design discussions, drop a note in the append-write channel or ping the storage team.
