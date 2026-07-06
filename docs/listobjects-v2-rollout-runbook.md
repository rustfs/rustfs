# ListObjectsV2 Optimization Rollout Runbook

This runbook defines the operator and maintainer contract for ListObjectsV2
source modes. The default RustFS ListObjectsV2 path remains walker-backed unless
an explicit opt-in mode is configured.

## Source Modes

| Mode | Source label | Default | Metadata authority | Consistency contract | Serving status |
| --- | --- | --- | --- | --- | --- |
| Walker | `walker` | Yes | Live `xl.meta` | Strong default ListObjectsV2 path | Enabled |
| Key-only index | `index_key_only` | No | Index proposes keys; live `xl.meta` verifies objects | Strong only after live verification | Prototype gate only |
| Verified-page index | `index_verified_page` | No | Index proposes pages; live `xl.meta` verifies objects | Strong only after live verification | Prototype gate only |
| Metadata-fast index | `index_metadata_fast` | No | Index snapshot metadata | Eventually consistent only | Phase 7 prototype serving gate only |

Never present `index_metadata_fast` as equivalent to the default S3-compatible
walker path. It requires a separate staleness SLA, chaos evidence, and an
immediate rollback path.

## Feature Flags

Current opt-in gate:

```bash
RUSTFS_LIST_OBJECTS_INDEX_MODE=index_key_only
RUSTFS_LIST_OBJECTS_INDEX_MODE=key_only
RUSTFS_LIST_OBJECTS_INDEX_MODE=index_verified_page
RUSTFS_LIST_OBJECTS_INDEX_MODE=verified_page
```

Unset or unknown values keep the default walker path.

Metadata-fast requires a separate experimental gate and an explicit staleness
SLA. It must not be enabled through the key-only or verified-page flag:

```bash
RUSTFS_LIST_OBJECTS_INDEX_MODE=index_metadata_fast
RUSTFS_LIST_OBJECTS_METADATA_FAST_ENABLED=true
RUSTFS_LIST_OBJECTS_METADATA_FAST_STALENESS_MS=5000
```

The staleness budget must be between `1` and `60000` milliseconds. Missing,
disabled, invalid, or over-budget metadata-fast settings keep the default walker
path. The current Phase 7 prototype only serves metadata-fast from the
`persistent_key_only` provider after a live walker rebuild has written metadata
snapshot rows into the persistent index.

Metadata-fast must fall back to walker when any of these guardrails fail:

- provider is missing or is not `persistent_key_only`
- namespace mutation journal is degraded or unreadable
- index checkpoint does not cover the current journal high-water mark
- continuation token generation does not match the active index generation
- persistent index was created by an older key-only writer and lacks complete
  metadata snapshot rows

## Immediate Rollback

To return all ListObjectsV2 requests to the default walker path:

1. Remove `RUSTFS_LIST_OBJECTS_INDEX_MODE`.
2. Restart or reload the affected RustFS process according to the deployment
   system.
3. Confirm that ListObjectsV2 fallback/index counters stop increasing.
4. Confirm normal walker gather/merge metrics continue to update.

No data migration is required to roll back because index-backed modes are
opt-in and the walker path remains authoritative.

## Required Metrics

Current implemented metrics:

These metrics are gated by `rustfs_io_metrics::get_stage_metrics_enabled()` to keep the default ListObjects hot path lightweight when detailed GET stage metrics are disabled.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `rustfs_s3_list_objects_gather_total` | counter | `source`, `outcome`, `has_prefix`, `has_delimiter`, `has_marker` | Gather attempts and request shape |
| `rustfs_s3_list_objects_gather_duration_ms` | histogram | `source`, `outcome` | Gather latency |
| `rustfs_s3_list_objects_gather_scanned_entries` | histogram | `source` | Entries scanned before page output |
| `rustfs_s3_list_objects_gather_returned_entries` | histogram | `source` | Entries returned before pagination trimming |
| `rustfs_s3_list_objects_gather_filtered_entries` | histogram | `source` | Entries filtered before page output |
| `rustfs_s3_list_objects_gather_scan_amplification` | histogram | `source` | Scanned-to-returned entry ratio |
| `rustfs_s3_list_objects_gather_limit` | histogram | `source` | Internal gather limit |
| `rustfs_s3_list_objects_merge_fan_in` | histogram | `source`, `outcome` | Merge input stream count |
| `rustfs_s3_list_objects_merge_read_quorum` | histogram | `source`, `outcome` | Merge read quorum |
| `rustfs_s3_list_objects_index_attempt_total` | counter | `source`, `provider`, `has_prefix`, `has_delimiter`, `has_marker` | Opt-in index serving attempts; denominator for fallback ratio |
| `rustfs_s3_list_objects_index_fallback_total` | counter | `source`, `reason` | Opt-in index attempts that fell back to walker |
| `rustfs_s3_list_objects_index_served_total` | counter | `source`, `provider`, `is_truncated` | Pages actually served by an opt-in index provider |
| `rustfs_s3_list_objects_index_candidate_keys` | histogram | `source`, `provider` | Candidate keys proposed by the opt-in provider per page |
| `rustfs_s3_list_objects_index_live_verify_attempts` | histogram | `source`, `provider` | Live xl.meta verification attempts per opt-in page |
| `rustfs_s3_list_objects_index_live_verify_hits` | histogram | `source`, `provider` | Candidates accepted by live verification per opt-in page |
| `rustfs_s3_list_objects_index_live_verify_misses` | histogram | `source`, `provider` | Stale/missing candidates rejected by live verification per opt-in page |
| `rustfs_s3_list_objects_index_live_verify_failure_total` | counter | `source`, `reason` | Live verification failures that surface as errors |
| `rustfs_s3_list_objects_index_returned_objects` | histogram | `source`, `provider` | Objects returned per opt-in page |
| `rustfs_s3_list_objects_index_returned_prefixes` | histogram | `source`, `provider` | Common prefixes returned per opt-in page |
| `rustfs_s3_list_objects_index_verification_io_amplification` | histogram | `source`, `provider` | Live verification attempts divided by returned objects |

Future index manager metrics must add:

- active source mode by bucket or cluster
- index lifecycle state: disabled, rebuilding, healthy, lagging, degraded,
  corrupt
- active generation and staging generation
- checkpoint high-water mark and mutation high-water mark
- mutation lag and lag threshold
- persistent index lifecycle state by bucket or shard
- generation checkpoint publish/lag gauges
- CPU and allocator attribution for walker vs opt-in verified serving

## Alert Guidelines

Recommended alerts before any index-backed serving rollout:

| Condition | Severity | Action |
| --- | --- | --- |
| `index_fallback_total` increases after opt-in | Warning | Inspect `reason`; stay on walker if fallback dominates |
| Lifecycle state is `rebuilding` longer than the rebuild SLO | Warning | Check rebuild worker progress and disk errors |
| Lifecycle state is `lagging` | Warning or critical by lag | Roll back if lag exceeds the staleness budget |
| Lifecycle state is `degraded` | Critical | Roll back to walker and inspect health reason |
| Lifecycle state is `corrupt` | Critical | Roll back, discard generation, rebuild from walker/live metadata |
| Metadata-fast enabled without SLA evidence | Critical | Disable immediately |
| Metadata-fast fallback reason is `metadata_fast_unavailable` | Warning | Rebuild the persistent provider with metadata snapshot rows |
| Metadata-fast serves while lifecycle is lagging/degraded/corrupt | Critical | Disable immediately and inspect journal/checkpoint guardrails |

## Compatibility Matrix

| Workload / feature | Walker | Key-only live-verified | Verified-page live-verified | Metadata-fast eventually-consistent |
| --- | --- | --- | --- | --- |
| Unversioned bucket | Supported | Requires live verification | Requires live verification | Requires SLA |
| Versioned bucket | Supported | Requires version-aware verifier | Requires version-aware verifier | Not allowed until chaos-proven |
| Delete marker | Live authoritative | Live verifier must suppress stale objects | Live verifier must suppress stale objects | High risk; requires chaos evidence |
| Overwrite | Live authoritative | Live verifier returns latest visible object | Live verifier returns latest visible object | Requires bounded staleness SLA |
| Multipart complete | Live authoritative | Must verify completed object metadata | Must verify completed object metadata | Requires mutation journal proof |
| Delimiter/common prefixes | Supported | Candidate page must preserve prefix boundary | Candidate page must preserve prefix boundary | Requires pagination SLA |
| Continuation token | Walker cursor | Cursor must carry source/generation | Cursor must carry source/generation | Cursor must carry source/generation and staleness semantics |
| Crash during rebuild | Walker unaffected | Must fall back if generation unpublished | Must fall back if generation unpublished | Must discard or reconcile staging generation |
| Lagging index | Walker unaffected | Fall back walker | Fall back walker | Must expose stale-read contract and rollback |

## Release Checklist

Do not broaden rollout unless every item below has evidence attached to the PR
or release tracker.

- `cargo test -p rustfs-io-metrics list_objects_metrics`
- `cargo test -p rustfs-ecstore list_index`
- `cargo test -p rustfs-ecstore verified_index_candidates`
- `cargo test -p rustfs-ecstore metadata_snapshot`
- `cargo test -p rustfs-ecstore metadata_fast`
- `cargo test -p rustfs-ecstore list_objects`
- `cargo fmt --all --check`
- Benchmark large bucket walker baseline.
- Benchmark opt-in verified mode p50, p95, p99.
- Benchmark metadata-fast mode p50, p95, p99 with the same bucket and release
  binary.
- Record fallback ratio and fallback reasons.
- Record live verification IO amplification.
- Record CPU and allocation delta.
- Run overwrite/delete/multipart-complete adversarial tests.
- Run crash during rebuild and restart recovery tests.
- Confirm rollback by unsetting `RUSTFS_LIST_OBJECTS_INDEX_MODE`.

The current executable benchmark and chaos entrypoint is:

```bash
scripts/run_listobjects_verified_bench.sh \
  --release \
  --objects 100000 \
  --duration 60s \
  --warmup-duration 20s
```

It writes per-mode `list-summary.csv` files with p50/p95/p99 latency,
`index-ratios.csv` files with fallback ratio and verification amplification,
`resource-summary.csv` files with process CPU/RSS attribution, Prometheus
allocator snapshots when available, and
`metadata_fast/chaos/chaos-summary.csv` for overwrite/delete/multipart plus
cross-page fallback continuity evidence. The chaos probe uses a small
independent page size (`--chaos-max-keys`, default `3`) and compares the old
continuation-token page against a fresh post-mutation listing slice to prove no
duplicate and no skipped keys across the fallback boundary.

## Canary Gates

Start with a bucket or cluster where stale listing risk is acceptable and
operator coverage is active.

Canary may proceed only if:

- default walker correctness remains unchanged
- fallback reasons are visible
- fallback ratio is below the rollout threshold
- p95 and p99 latency do not regress beyond the agreed SLO
- live verification IO amplification is understood
- no stale delete marker, stale overwrite, or multipart completion regression is
  observed
- metadata-fast canary has an explicit staleness SLA and rollback owner

Canary must stop immediately if:

- lifecycle state becomes degraded or corrupt
- mutation lag exceeds the configured staleness budget
- continuation tokens duplicate or skip objects
- metadata-fast appears on a strong-consistency response path
- metadata-fast serves without a healthy journal, matching generation, complete
  metadata snapshot, and checkpoint coverage

## Maintainer Notes

The verified index path is allowed to accelerate candidate discovery only. It
must not become a metadata authority. Any change that returns object metadata
directly from an index snapshot belongs to metadata-fast and must use the
eventually-consistent contract with separate feature flags, documentation,
chaos tests, and rollback evidence.
