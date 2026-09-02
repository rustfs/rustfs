# Internode msgpack/JSON convergence runbook

**Use this when:** operating or changing the staged retirement of the JSON compatibility fields on internode gRPC metadata RPCs, deciding whether a fleet may enable `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY`, or adding a new `*_bin` proto field.
**Source of truth:** `crates/config/src/constants/internode.rs` (`ENV_INTERNODE_RPC_MSGPACK_ONLY`, `ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED` and their compile-time default-off asserts), `crates/protos/src/node.proto` (`*_bin` fields), `decode_msgpack_or_json` in `rustfs/src/storage/rpc/node_service/disk.rs` (server side) and `crates/ecstore/src/cluster/rpc/remote_disk.rs` (client side), `crates/io-metrics/src/internode_metrics.rs` (counters).

This is a cross-version change and proceeds by observation-gated stages, never in one step.

## Background

Internode RPCs dual-encode each metadata value as a msgpack binary field (`*_bin`, for example `file_info_bin`) and a JSON compatibility string (for example `file_info`). Decoders prefer `_bin` and fall back to JSON only when `_bin` is empty. The dual-write costs bandwidth and CPU; before the JSON fields can be dropped, the fallback branch must be proven unused across the fleet, otherwise a mixed-version rolling upgrade could read an emptied field.

## Observation counters

| Counter | Labels | Increments when |
| --- | --- | --- |
| `rustfs_system_network_internode_msgpack_json_decode_total` | `direction`, `message`, `codec` | a msgpack or JSON decode succeeded |
| `rustfs_system_network_internode_msgpack_json_fallback_total` | `direction`, `message` | a decode fell back to JSON because `_bin` was empty |
| `rustfs_system_network_internode_msgpack_json_decode_error_total` | `direction`, `message`, `codec` | either codec failed to decode (`codec` = the failed codec) |

`direction="request"` is a server decoding a peer's request (`node_service/disk.rs`); `direction="response"` is a client decoding a peer's response (`cluster/rpc/remote_disk.rs`), including the list-level `ReadMultiple` / `BatchReadVersion` fallbacks. `message` is the value name (`FileInfo`, `RawFileInfo`, `ReadMultipleResp`, ...).

Gate query template, run over the whole observation window (adjust `[30d]`):

```promql
sum by (direction, message, codec) (increase(<counter>[30d]))
```

| Counter | Required reading | Meaning of a violation |
| --- | --- | --- |
| `..._decode_total` | every convergence-ready `{direction, message}` has a non-zero `codec="msgpack"` series | no traffic, a counter reset, or scrape gaps make the gate inconclusive, not passed |
| `..._fallback_total` | `0` for every series | some peer still sends an empty `_bin` (old node, or a sender that does not fill `_bin`); investigate the labels |
| `..._decode_error_total` | `0` for every series | `codec="msgpack"`: corrupt or incompatible `_bin` bytes; `codec="json"`: corrupt legacy fallback. Either blocks convergence and rollback confidence |

Standing alerts (keep enabled through every stage):

```yaml
- alert: InternodeMsgpackJsonFallback
  expr: sum by (direction, message) (increase(rustfs_system_network_internode_msgpack_json_fallback_total[15m])) > 0
  for: 5m
  labels: { severity: warning }
  annotations:
    summary: "Internode RPC fell back to JSON decode ({{ $labels.direction }}/{{ $labels.message }})"
    description: "A peer sent an empty msgpack _bin payload. Do NOT advance msgpack-only convergence while this fires."
- alert: InternodeMsgpackJsonDecodeError
  expr: sum by (direction, message, codec) (increase(rustfs_system_network_internode_msgpack_json_decode_error_total[15m])) > 0
  for: 5m
  labels: { severity: warning }
  annotations:
    summary: "Internode RPC msgpack/JSON decode failed ({{ $labels.direction }}/{{ $labels.message }}/{{ $labels.codec }})"
    description: "A peer sent an undecodable msgpack or JSON compatibility payload. Do NOT advance msgpack-only convergence while this fires."
```

## Field → peer-decoder audit

Stage 1 may only empty a JSON field whose peer decodes `_bin` first. Any `*_bin` field not listed here must be mapped to a confirmed `_bin`-first peer decoder before it joins the convergence set.

Convergence-ready:

| Direction | Message / field | Peer decoder |
| --- | --- | --- |
| request | `WriteMetadata.file_info` | `FileInfo` (`node_service/disk.rs`) |
| request | `UpdateMetadata.file_info` | `FileInfo` |
| request | `UpdateMetadata.opts` | `UpdateMetadataOpts` |
| request | `RenameData.file_info` | `FileInfo` |
| request | `ReadMultiple.read_multiple_req` | `ReadMultipleReq` |
| request | `BatchReadVersion.batch_read_version_req` | `BatchReadVersionReq` |
| request | `Read*.opts` | `ReadOptions` |
| response | `ReadVersion.file_info` | `FileInfo` (`cluster/rpc/remote_disk.rs`) |
| response | `ReadXL.raw_file_info` | `RawFileInfo` |
| response | `RenameData.rename_data_resp` | `RenameDataResp` |
| response | `ReadMultiple` response list | per-item plus list fallback |
| response | `BatchReadVersion` response list | per-item plus list fallback |

Delete messages, kept on dual-write until their own window reads zero with `_bin`-first decoders deployed fleet-wide (the client always dual-writes these regardless of the flags):

| Direction | Message / field | Status |
| --- | --- | --- |
| request | `DeleteVersion.file_info` (`FileInfo`) | `_bin` present; dual-write; converge after its own window |
| request | `DeleteVersion.opts` (`DeleteOptions`) | same |
| request | `DeleteVersions.versions` (`FileInfoVersions`) | same |
| request | `DeleteVersions.opts` (`DeleteOptions`) | same |

Still JSON-only:

| Direction | Message / field | Note |
| --- | --- | --- |
| response | `DeleteVersion.raw_file_info` | proto has no `_bin`; needs an additive proto field before it can converge |

## Stage 0 — Observe

Run the fleet with both flags at their default (`false`) for at least one full observation window and evaluate the three gate rows above. All three must hold before Stage 1.

## Stage 1 — Stop writing JSON (env-gated)

Flag semantics are defined on `ENV_INTERNODE_RPC_MSGPACK_ONLY` and `ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED` in `crates/config/src/constants/internode.rs`; both default off and only their conjunction stops the JSON write for convergence-ready fields. Decoders keep the JSON fallback in every state.

1. Rehearse `request-only`: `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true`, `..._FLEET_CONFIRMED=false`. Behaviour is unchanged (still dual-write) and safe with old peers; any fallback or decode-error increment still blocks.
2. Canary: set both flags `true` on one node, restart that node only, and watch the fallback and decode-error counters for a soak period with real internode traffic.
3. Fleet: if the counters stay zero, enable both flags fleet-wide with a rolling restart.
4. Rollback: set either flag to `false` (or unset it) and restart; no wire format changed, so rollback is immediate.

The benchmark driver pins these operator states as dry-run phases (`before`, `request-only`, `canary`, `after`, `rollback`); see [internode-grpc-benchmark-runbook.md](internode-grpc-benchmark-runbook.md).

## Stage 2 — Remove the proto JSON fields

Only after Stage 1 has been stable fleet-wide for a full window with the counters still zero.

1. Mark the retired text fields `reserved` in `crates/protos/src/node.proto` (never reuse field numbers) and delete the JSON read-fallback branches.
2. This is a hard wire-format change: it requires the mixed-version upgrade rehearsal (four-node scripts) to pass and cannot be rolled back by env alone.

## Rollback matrix

| Stage | Wire format broken? | Rollback |
| --- | --- | --- |
| 0 Observe | no | n/a (metrics only) |
| 1 msgpack-only send | no | unset either flag and restart |
| 2 remove fields | yes | redeploy the prior release; field numbers stay `reserved` |

## Related

- Transport and codec observability landed in `feat(internode): optimize gRPC transport (#4337)`.
