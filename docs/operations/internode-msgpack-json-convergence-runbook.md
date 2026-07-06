# Internode msgpack/JSON Convergence Runbook

Operational runbook for retiring the redundant JSON compatibility fields on internode
gRPC metadata RPCs (grpc-optimization **P2-1**). This is a **cross-version** change: it
proceeds strictly by observation-gated stages, never in one step.

## Background

Internode RPCs dual-encode each metadata value as **both**:

- a msgpack binary field (`*_bin`, e.g. `file_info_bin`), and
- a JSON compatibility string (e.g. `file_info`).

Decoders prefer the `_bin` payload and fall back to the JSON string only when `_bin` is
empty (`decode_msgpack_or_json`). The dual-write costs bandwidth and CPU. Before the JSON
fields can be dropped, the fallback branch must be proven **unused** in production —
otherwise a rolling upgrade with mixed node versions could read an emptied field.

## The observation metric (already shipped)

```
rustfs_system_network_internode_msgpack_json_fallback_total{direction, message}
```

Incremented whenever a decode falls back to the JSON field because the msgpack payload was
absent.

- `direction="request"` — a server decoding a peer's request (`node_service/disk.rs`).
- `direction="response"` — a client decoding a peer's response (`cluster/rpc/remote_disk.rs`),
  including the list-level `ReadMultiple` / `BatchReadVersion` fallbacks.
- `message` — the value name, e.g. `FileInfo`, `RawFileInfo`, `ReadMultipleResp`.

## Stage 0 — Observe (current stage)

Ship the current release (which contains the counter) and let it run for **at least one
full release window** across the whole fleet. The counter must stay at **zero**.

Confirm zero across the observation window (adjust `[30d]` to the window length):

```promql
sum by (direction, message) (
  increase(rustfs_system_network_internode_msgpack_json_fallback_total[30d])
)
```

Every series must be `0`. A non-zero value means some peer is still emitting an empty
`_bin` (an old node, or a message whose sender does not fill `_bin`) — investigate the
`{direction, message}` label before proceeding.

Standing alert (keep enabled through all stages):

```yaml
- alert: InternodeMsgpackJsonFallback
  expr: sum by (direction, message) (increase(rustfs_system_network_internode_msgpack_json_fallback_total[15m])) > 0
  for: 5m
  labels: { severity: warning }
  annotations:
    summary: "Internode RPC fell back to JSON decode ({{ $labels.direction }}/{{ $labels.message }})"
    description: "A peer sent an empty msgpack _bin payload. Do NOT advance msgpack-only convergence while this fires."
```

## Field → peer-decoder audit

The send-side change (Stage 1) may only empty a JSON field whose **peer decodes `_bin`
first**. The following mapping is verified against the current code.

### Convergence-ready (peer decodes `_bin` first)

| Direction | Message / field | Peer decoder |
|---|---|---|
| request | `WriteMetadata.file_info` | `FileInfo` (node_service/disk.rs) |
| request | `UpdateMetadata.file_info` | `FileInfo` |
| request | `UpdateMetadata.opts` | `UpdateMetadataOpts` |
| request | `RenameData.file_info` | `FileInfo` |
| request | `ReadMultiple.read_multiple_req` | `ReadMultipleReq` |
| request | `BatchReadVersion.batch_read_version_req` | `BatchReadVersionReq` |
| request | `Read*.opts` | `ReadOptions` |
| response | `ReadVersion.file_info` | `FileInfo` (cluster/rpc/remote_disk.rs) |
| response | `ReadXL.raw_file_info` | `RawFileInfo` |
| response | `RenameData.rename_data_resp` | `RenameDataResp` |
| response | `ReadMultiple` resp list | per-item + list fallback |
| response | `BatchReadVersion` resp list | per-item + list fallback |

### NOT convergence-ready (no confirmed `_bin`-first peer decoder)

| Direction | Message / field | Note |
|---|---|---|
| request | `DeleteVersion(s).opts` (`DeleteOptions`) | handler is not `_bin`-first; **upgrade the decoder to `decode_msgpack_or_json` first**, ship it a window, then converge. |

> Any `*_bin` proto field not in the tables above must be mapped to a confirmed `_bin`-first
> peer decoder before it is added to the convergence set.

## Stage 1 — Stop writing JSON (env-gated, after Stage 0 reads zero)

Only after the counter has read zero for a full window across the fleet.

1. Land the send-side change gated by a **default-off** env flag
   (`RUSTFS_INTERNODE_RPC_MSGPACK_ONLY`, default `false`): when enabled, the
   convergence-ready fields above send only `_bin` and leave the JSON string empty. The
   `_bin` payload is always sent; decoders keep the JSON read fallback unchanged.
2. Roll out with the flag **off** (no behavior change), then enable it on one node and
   watch the fallback counter for a soak period. If it stays zero, enable fleet-wide.
3. **Rollback:** set `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=false` (or unset) and restart. No
   wire-format was broken in this stage, so rollback is immediate and safe.

## Stage 2 — Remove the proto JSON fields (next release, N+1)

Only after Stage 1 has been stable with the flag on for a full window and the counter is
still zero.

1. Mark the retired text fields `reserved` in `crates/protos/src/node.proto` (never reuse
   the field numbers) and delete the JSON read-fallback branches; codec becomes msgpack-only.
2. This is a hard wire-format change — it requires the mixed-version upgrade rehearsal
   (four-node scripts) to pass, and it cannot be rolled back by env alone.

## Rollback matrix

| Stage | Wire-format broken? | Rollback |
|---|---|---|
| 0 Observe | no | n/a (metric only) |
| 1 msgpack-only send | no | `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=false` + restart |
| 2 remove fields | yes | redeploy prior release; field numbers stay `reserved` |

## Related

- Codec + counter implementation: commit `feat(internode): P2 msgpack/JSON codec observability + encode buffer presizing`.
- Decoders: `decode_msgpack_or_json` in `crates/ecstore/src/cluster/rpc/remote_disk.rs` (client)
  and `rustfs/src/storage/rpc/node_service/disk.rs` (server).
