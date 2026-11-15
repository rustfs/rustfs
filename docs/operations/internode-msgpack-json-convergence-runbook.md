# Internode msgpack/JSON convergence runbook

**Use this when:** operating or changing the staged retirement of the JSON compatibility fields on internode gRPC metadata RPCs, deciding whether a fleet may enable `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY`, or adding a new `*_bin` proto field.
**Source of truth:** `crates/config/src/constants/internode.rs` (`ENV_INTERNODE_RPC_MSGPACK_ONLY`, `ENV_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED`, `ENV_INTERNODE_RPC_MSGPACK_PEER_CAPABILITY` and their compile-time default-off asserts), `crates/protos/src/node.proto` (`*_bin` fields), `decode_msgpack_or_json` in `rustfs/src/storage/rpc/node_service/disk.rs` (server side) and `crates/ecstore/src/cluster/rpc/remote_disk.rs` (client side), `crates/io-metrics/src/internode_metrics.rs` (counters).

This is a cross-version change and proceeds by observation-gated stages, never in one step.

## Background

Internode RPCs dual-encode each metadata value as a msgpack binary field (`*_bin`, for example `file_info_bin`) and a JSON compatibility string (for example `file_info`). Decoders prefer `_bin` and fall back to JSON only when `_bin` is empty. The dual-write costs bandwidth and CPU; before the JSON fields can be dropped, the fallback branch must be proven unused across the fleet, otherwise a mixed-version rolling upgrade could read an emptied field.

## Observation counters

| Counter | Labels | Increments when |
| --- | --- | --- |
| `rustfs_system_network_internode_msgpack_json_decode_total` | `direction`, `message`, `codec` | a msgpack or JSON decode succeeded |
| `rustfs_system_network_internode_msgpack_json_fallback_total` | `direction`, `message` | a decode fell back to JSON because `_bin` was empty |
| `rustfs_system_network_internode_msgpack_json_decode_error_total` | `direction`, `message`, `codec` | either codec failed to decode (`codec` = the failed codec) |
| `rustfs_system_network_internode_msgpack_send_decisions_total` | `decision`, `reason` | an eligible RPC selected `msgpack_only` or `dual_write`; reasons are fixed to `manual_fleet`, `peer_capable`, `peer_unknown`, `peer_revoked`, or `not_requested` |

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

Flag semantics are defined in `crates/config/src/constants/internode.rs`; all three flags default off. `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true` requests msgpack-only, and JSON stops only when either fleet confirmation is true or the peer-capability gate is true and the exact current peer boot epoch plus physical connection generation has an authenticated capability proof. Unknown, unsupported, revoked, stale, or reconnecting peers continue dual-write. Decoders keep the JSON fallback in every state.

1. Rehearse `request-only`: `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true`, `..._FLEET_CONFIRMED=false`. Behaviour is unchanged (still dual-write) and safe with old peers; any fallback or decode-error increment still blocks.
2. Canary: set both flags `true` on one node, restart that node only, and watch the fallback and decode-error counters for a soak period with real internode traffic.
3. Fleet: if the counters stay zero, enable both flags fleet-wide with a rolling restart.
4. Rollback: set either flag to `false` (or unset it) and restart; no wire format changed, so rollback is immediate.

An alternative per-peer canary keeps `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY_FLEET_CONFIRMED=false`, sets `RUSTFS_INTERNODE_RPC_MSGPACK_ONLY=true` and `RUSTFS_INTERNODE_RPC_MSGPACK_PEER_CAPABILITY=true`, and confirms `reason="peer_capable"` only for proved peers while every `peer_unknown` or `peer_revoked` decision remains `dual_write`. Enable this mode only when one client HTTP/2 connection has stable affinity to one RustFS peer process; an L7 proxy that routes streams from one connection to different backend versions invalidates the connection-generation proof. A connection-generation change may rebuild one request as dual-write only when the retained local guard proves rejection before the first payload frame; remote status text, a dispatched request, or an ordinary transport error never authorizes that compatibility retry. Disable either the base request flag or peer-capability flag to roll back immediately.

The benchmark driver pins these operator states as dry-run phases (`before`, `request-only`, `canary`, `after`, `rollback`); see [internode-grpc-benchmark-runbook.md](internode-grpc-benchmark-runbook.md).

### Per-peer mixed-version evidence gate

The per-peer gate is not qualified by a successful command transcript alone. Record three ordered four-node measurement phases: `mixed-old-peer` (three candidate nodes and one previous-version node), `all-new` (four candidate nodes), and `rollback-old-peer` (replace one candidate with the previous-version image and verify readback). Every candidate node uses `MSGPACK_ONLY=true`, `MSGPACK_PEER_CAPABILITY=true`, and `MSGPACK_ONLY_FLEET_CONFIRMED=false`; previous-version nodes use all three false. Each phase must run real PUT/GET/metadata traffic and retain the exact candidate/baseline revisions, topology, timestamps, workload result, environment, and raw Prometheus HTTP API JSON.

Assign one unique `msgpack-<32-hex>` cluster ID for this run, set `OTEL_RESOURCE_ATTRIBUTES=rustfs.cluster.id=<id>` on every node, and retain the collector’s resource-to-telemetry conversion so Prometheus exposes `rustfs_cluster_id`. Capture these PromQL vectors over each non-overlapping phase window and store the complete HTTP API response, not extracted numbers:

```promql
sum by (decision, reason) (increase(rustfs_system_network_internode_msgpack_send_decisions_total{rustfs_cluster_id="$CLUSTER_ID"}[$WINDOW]))
sum(increase(rustfs_system_network_internode_msgpack_json_fallback_total{rustfs_cluster_id="$CLUSTER_ID"}[$WINDOW])) or vector(0)
sum(increase(rustfs_system_network_internode_msgpack_json_decode_error_total{rustfs_cluster_id="$CLUSTER_ID"}[$WINDOW])) or vector(0)
(sum(increase(rustfs_system_network_internode_operation_errors_total{rustfs_cluster_id="$CLUSTER_ID",backend="grpc",operation!="grpc_read_version"}[$WINDOW])) or vector(0)) + (sum(increase(rustfs_system_network_internode_operation_classified_errors_total{rustfs_cluster_id="$CLUSTER_ID",backend="grpc",operation="grpc_read_version",classification!="remote_response"}[$WINDOW])) or vector(0))
```

Build a schema-1 manifest with `scope="internode-msgpack-peer-canary"`, `source_revision`, `baseline_revision`, `stable_connection_affinity=true`, `max_metric_lag_secs`, `cluster_id`, an `images` path to sanitized image inspections, and the three phase records. Each phase records integer `start_unix`/`end_unix`, exact topology counts plus a `node1..node4` revision map, the new/old node environment maps, the exact four PromQL strings with a window equal to the phase duration, an `images` path to the verified per-node image ID/revision map, a relative workload JSON path, and relative paths for `decisions`, `fallback`, `decode_errors`, and `rpc_errors`. The workload artifact records `status`, a non-zero integer `requests`, and `rollback_readback` (true only for `rollback-old-peer`). Validate without rewriting raw evidence:

```bash
python3 scripts/check_internode_msgpack_peer_canary.py \
  --manifest /path/to/evidence/manifest.json \
  --out /path/to/evidence/verification.json
```

The verifier rejects reused/escaping/overwritten artifacts, duplicate or non-exact decision labels, labeled scalar queries, mismatched PromQL windows, wrong per-node revisions, missing peer-capable traffic, missing mixed/rollback dual-write traffic, manual-fleet or not-requested leakage, failed workload/readback, non-zero fallback/decode/gRPC transport-or-protocol errors, stale timestamps, invalid topology, and candidate/baseline identity reuse. The gRPC error query checks every operation except `grpc_read_version` through the total error counter. Read-version uses its classified counter and excludes only `remote_response`, because a successful gRPC exchange can carry an expected application-level disk response; `grpc_status` and `response_decode` still block the gate. TCP/HTTP operation outcomes remain outside this msgpack gRPC compatibility gate and the workload's zero-error contract remains independently enforced. The workload artifact binds its raw benchmark `summary.csv` by relative path and SHA-256; rollback also records equal source/download content hashes. Verifier output includes SHA-256 receipts for every raw query, workload, and summary artifact. Passing this gate is compatibility evidence only; production soak and ABBA performance drift gates remain separate.

Use the orchestration runner to create the isolated Compose project, execute the three phases, preserve a known object across node2 rollback, capture the four raw Prometheus responses, build the manifest, and invoke the verifier. Without `--execute` it prints a redacted plan and makes no external changes. The runner verifies distinct immutable image IDs and full `vcs-ref` or OCI revision labels against the requested revisions, pins Compose to those IDs, and checks each running container’s image and telemetry identity in every phase. If an image backend omits both recognized labels from `image inspect`, the runner queries `image history` by the already resolved immutable image ID and accepts only a canonical 40-hex revision from the newest matching `LABEL` instruction; an existing mismatched config label never falls back. Images without matching full revision evidence are rejected. It records whether each sanitized identity receipt came from config labels or immutable history without retaining container credentials or unrelated history. Execution refuses an existing Compose project/volume set and removes its isolated containers and volumes on exit unless `--keep-up` is explicit.

```bash
scripts/run_internode_msgpack_peer_canary.sh \
  --candidate-image rustfs/rustfs:candidate \
  --baseline-image rustfs/rustfs:previous \
  --candidate-revision <40-hex-candidate-commit> \
  --baseline-revision <40-hex-previous-commit> \
  --prometheus-query-url http://127.0.0.1:9090/api/v1/query \
  --out-dir /path/to/evidence \
  --execute
```

The runner requires Docker Compose, AWS CLI, curl, Warp through `run_object_batch_bench.sh`, Python, the four-node local cluster compose file, prebuilt candidate/baseline images, and an already reachable Prometheus/OTLP pipeline. It never builds, pulls, or starts observability images implicitly. Execution rejects reused Compose resources or a non-empty evidence directory and performs a Prometheus vector preflight before starting the cluster. The workload summary must contain only successful zero-error rows with parseable request rates; the rollback phase must read byte-identical object data written before the version replacement.

## Stage 2 — Remove the proto JSON fields

Only after Stage 1 has been stable fleet-wide for a full window with the counters still zero.

1. Mark the retired text fields `reserved` in `crates/protos/src/node.proto` (never reuse field numbers) and delete the JSON read-fallback branches.
2. This is a hard wire-format change: it requires the mixed-version upgrade rehearsal (four-node scripts) to pass and cannot be rolled back by env alone.

## Rollback matrix

| Stage | Wire format broken? | Rollback |
| --- | --- | --- |
| 0 Observe | no | n/a (metrics only) |
| 1 msgpack-only send | no | unset the base request flag, or unset the selected fleet/peer gate, and restart |
| 2 remove fields | yes | redeploy the prior release; field numbers stay `reserved` |

## Related

- Transport and codec observability landed in `feat(internode): optimize gRPC transport (#4337)`.
