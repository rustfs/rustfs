# Issue #4003 Pragmatic Execution Plan for ListObjectsV2 Performance

Date: 2026-06-29

Reference:
- rustfs issue: https://github.com/rustfs/rustfs/issues/4003
- backlog parent issue: https://github.com/rustfs/backlog/issues/788
- subtask: #785 #786 #787

## 1) Problem statement and observed failure mode
`ListObjectsV2` becomes very slow on very large buckets (~745k objects) under recursive listing workloads (`rclone size`, `rclone lsf --recursive`).

Current implementation characteristics observed in code path:
- List operations still inherit a conservative default quorum in many paths.
- Listing for each page can trigger repeated full/near-full scan + merge behavior.
- Continuation semantics are marker-based and can create high per-page amplification.

## 2) Root-cause hypothesis (five-perspective review)
### 2.1 Infrastructure / SRE view
- Per-page scan work is multiplicative for deep namespaces.
- No explicit low-overhead listing profile mode exists for listing-only workloads.
- Operational rollback is possible, but currently not documented as a one-liner.

### 2.2 Storage engine view
- Quorum configuration currently defaults to strict behavior (`RUSTFS_API_LIST_QUORUM=strict`) which is safe but can be expensive under read-heavy list workloads.
- Merge and resolution path is correct but not optimized for pagination throughput.

### 2.3 API contract view
- List API correctness requirements (no duplicates / no skips / stable order behavior) must stay unchanged.
- Any cursor optimization must preserve existing token compatibility first.

### 2.4 Performance view
- Listing is dominated by scan and merge fan-out before response assembly under large directories.
- Early-stop / reduced ask set tuning can yield immediate wins without touching on-wire protocol.

### 2.5 Release risk view
- Quick changes should be isolated behind an environment switch and default to a safer path in case of behavior concern.
- Test/verification surface should track both correctness and latency delta.

## 3) Fixes completed in current patch (Phase-1 scope)
### 3.1 Introduce ListObjects-only quorum switch
File: `crates/ecstore/src/store/list_objects.rs`
- Added constants:
  - `RUSTFS_LIST_OBJECTS_QUORUM`
  - default `optimal`
- Added helper:
  - `list_objects_quorum_from_env()`
- Switched ask disks for listing operations to use this helper in 6 call sites:
  - `ECStore::list_objects`
  - `ECStore::list_object_versions`
  - `Sets::list_objects`
  - `Sets::list_object_versions`
  - `SetDisks::list_objects`
  - `SetDisks::list_object_versions`

### 3.2 Unit tests added
In `#[cfg(test)]` for `list_objects.rs`:
- `list_objects_quorum_from_env_defaults_to_optimal`
- `list_objects_quorum_from_env_honors_supported_value`

These verify default and explicit env behavior.

### 3.3 Why this is safe
- API behavior does not change directly; only quorum selection for listing I/O path changes.
- Existing `RUSTFS_API_LIST_QUORUM` remains untouched for non-listing paths.
- Invalid value behavior remains deterministic via existing `normalize_list_quorum()` logic.

## 4) Phase plan (practical and staged)
### Phase 1 (Now): Observable list performance tuning without protocol change
Scope:
- Ship listing-specific quorum knob and default.
- Add metrics/logging for per-page scan count and early-stop ratio.
- Run baseline and post-change micro-bench on recursive list.

Acceptance:
- No correctness regression in list responses.
- Reduction in list CPU + elapsed time under large recursive reads.
- Rollback confirmed by setting `RUSTFS_LIST_OBJECTS_QUORUM=strict`.

### Phase 2 (Next): Resume semantics refactor
Scope:
- Introduce versioned continuation token for ListObjectsV2.
- Keep legacy token compatibility in parsing path.
- Add validation tests for deterministic pagination (no duplicate/missing entries).

Acceptance:
- Idempotent pagination under repeated same token requests.
- Zero regression in API contract and clients.
- Graceful fallback for unknown token format.

### Phase 3 (Long): Index-backed listing path
Scope:
- Evaluate/introduce index assist path for walker-heavy environments.
- Fallback to walker on index inconsistency.
- Define rebuild + repair strategy and runbooks.

Acceptance:
- Sustained throughput gains on very large buckets.
- Operational risk under consistency faults bounded.
- Rollback and recovery steps documented and tested.

## 5) Validation checklist (before merge)
- `cargo test -p rustfs-ecstore list_objects -- --nocapture` (focused on list code)
- `cargo test -p rustfs-ecstore list_objects_quorum_from_env` (if supported selector)
- `scripts/validate_issue_787_list_quorum.sh` (offline) and `scripts/validate_issue_787_list_quorum.sh --full` (live, if environment ready)
- Add/verify benchmark baseline:
  - recursive `rclone lsf --recursive` on staging bucket
  - `rclone size` style pass
- Compare:
  - p99 listing latency
  - request count per page
  - duplicated/missing keys between consecutive pages
  - CPU and disk read amplification

## 6) #787 live benchmark and rollout acceptance (in progress)

### 6.1 live benchmark plan (execution contract)

- Workload baseline:
  - Bucket: pre-populated production-equivalent bucket (>= 700k objects, mixed prefixes, deep tree)
  - Tool: `rclone lsf --recursive` + `rclone size`
  - Endpoint: `RUSTFS_ADDRESS=...` on fresh RustFS node with `RUSTFS_ACCESS_KEY`, `RUSTFS_SECRET_KEY`
  - Pages: `--max-keys` at 1000 / 5000 to measure p95/p99 page behavior

- Comparative configurations (at least two runs):
  - Baseline: `RUSTFS_LIST_OBJECTS_QUORUM=strict`
  - Candidate: `RUSTFS_LIST_OBJECTS_QUORUM=optimal` (or `reduced` if strict is unavailable in workload)

- Required outputs for each run:
  - p95/p99 page latency
  - wall-clock time for complete recursive listing pass
  - duplicates/missing count by token replay check
  - per-page key count and continuation token progression

- `scripts/validate_issue_787_list_quorum.sh --full`
  - set `MODE_LIST` to the candidate matrix, for example:
    - `MODE_LIST=strict,optimal,reduced`
  - export `SERVER_RESTART_CMD` and `SERVER_WAIT_SECONDS=20` to produce mode-scoped logs under one run output directory.

- Acceptance thresholds (proposal):
  - Candidate p99 page latency delta ≤ +10% and ≥ +10% wall-clock gain against baseline on same bucket class
  - Zero duplicates/missing objects across page transitions
  - No listing correctness regressions

### 6.2 completed local benchmark package (2026-06-30)

Benchmark environment used to close the missing evidence gap:
- single-node local RustFS with the same data directory reused across mode restarts
- bucket: `issue787-bench`
- object layout: 6,000 zero-byte objects across mixed recursive prefixes:
  - `alpha/obj-*`
  - `beta/deep/obj-*`
  - `gamma/nested/path/obj-*`
- request shape: full bucket recursive `ListObjectsV2`
- pagination: `max-keys=1000`
- sample size: 15 full passes per mode, 6 pages per pass, 90 page samples per mode

Results summary:

| Mode | Page p95 (ms) | Page p99 (ms) | Wall avg (ms) | p99 delta vs strict | Wall avg delta vs strict | Continuity |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `strict` | 255.413 | 275.438 | 1469.938 | baseline | baseline | pass |
| `optimal` | 259.146 | 267.342 | 1485.912 | -2.939% | +1.087% | pass |
| `reduced` | 255.937 | 272.557 | 1477.955 | -1.046% | +0.545% | pass |

Decision from the benchmark package:
- Both candidate modes stayed within the strict-baseline p99 guardrail in this local harness.
- Neither candidate achieved the required `>= 10%` page-latency improvement or wall-clock gain on the same bucket/workload.
- Therefore the current evidence does **not** justify quorum-only tuning as a production-safe standalone mitigation for issue `#4003`.
- Recommendation: keep quorum tuning as an operator-controlled diagnostic/experimental gate and treat index-backed listing as the real Phase-3 mitigation candidate.

### 6.3 execution attempt log

- Attempt start/run status:
  - `scripts/restart_local_single_node_multidisk_rustfs.sh` and direct `target/debug/rustfs server` launch path
  - Endpoint bind error observed: `Operation not permitted (os error 1)` on both 9000 and 19000
  - Artifact directory:
    - `target/issue-787-live-benchmark-20260630-031521/`

- Current status:
  - Live benchmark for #787 is **blocked by environment bind permission** in this runner.
  - Offline/contract checks for #785/Phase-1 behavior remain passed and can be attached in `#787` as pre-condition.

### 6.4 rollout documentation package (required to close #787)

- Add rollout control:
  - Config default: `RUSTFS_LIST_OBJECTS_QUORUM=optimal` (current patch)
  - Safety fallback: `RUSTFS_LIST_OBJECTS_QUORUM=strict`
  - Error budget guardrails for rollout (phase plan):
    - p99 latency +20% ceiling
    - error-rate +10% ceiling
    - continuity test zero duplicate/missing

- Release sequencing:
  - Stage A: shadow traffic for large-recursive listing buckets (same object layout + namespace)
  - Stage B: partial bucket/range enablement with continuity probes
  - Stage C: scale to wider prefix families only after two consecutive acceptance windows pass

### 6.5 index-backed engineering scaffold (next milestone)

- Scope: introduce an optional index-assisted listing mode with walk-based fallback.
- Minimal state machine design:
  1. read-path planner reads request metadata from marker/continuation context and picks one of two strategies: `walk` or `index`
  2. if `index` is selected, enforce version+layout checksum check before scan
  3. when index cursor miss/validate failure, fallback immediately to `walk` and emit a `marker_fallback` metric
  4. emit cursor and source tags in `NextContinuationToken` only when index strategy was used
- Acceptance for first pass:
  - zero functional regressions (`duplication=0`, `missing=0`) in offline smoke and token replay tests
  - fallback event rate below threshold in steady-state run (target: < 1%)
  - rollback via existing `RUSTFS_LIST_OBJECTS_QUORUM` control remains unchanged
- Close-out action: keep this section as the live action list in the follow-up issue/subtask and bind each item to a dedicated PR before closure.

### 6.6 explicit index-backed design artifact

Continuation-token compatibility matrix:

| Token family | Current/Planned producer | Parser action | Preferred read path | Fallback behavior |
| --- | --- | --- | --- | --- |
| legacy marker-only | legacy clients / pre-v2 servers | parse as legacy continuation | walker | fail closed only on malformed XML/query |
| v2 cache-tag token | current Phase-2 implementation | parse marker + cache tag, preserve replay semantics | walker with metacache resume | force cache refresh on corrupt/replay mismatch |
| proposed v3 index-cursor token | future index-assisted planner | parse marker + index cursor metadata + manifest epoch | index | immediate downgrade to walker on token parse / epoch / checksum mismatch |

Proposed index schema (design only, not implemented in this PR):
- `bucket`
- `prefix_scope`
- `object_key`
- `version_id` (optional for versioned buckets)
- `delete_marker`
- `etag`
- `size`
- `last_modified`
- `cursor_seq`
- `manifest_epoch`
- `record_checksum`

Proposed write-path hooks:
- `PutObject`, `CopyObject`, `CompleteMultipartUpload`: emit index upsert records
- `DeleteObject`, `DeleteObjectVersion`, lifecycle expiry: emit tombstone records
- replication/heal/rebuild workflows: emit replayable repair events into the same mutation stream

Consistency, backfill, and rebuild plan:
- initial build trigger: bucket/range enrollment into index-assisted listing
- backfill model: prefix-ordered walker snapshot with persisted watermark per bucket/range
- drift detection: periodic sampled compare between index page output and walker page output using the same continuation boundary
- rebuild trigger:
  - manifest epoch mismatch
  - record checksum mismatch
  - fallback rate above threshold
  - operator-requested rebuild after repair/heal incidents
- proposed operator tooling surface (design-only names):
  - `list-index build`
  - `list-index verify`
  - `list-index rebuild`
  - `list-index repair`

Fallback state machine requirements:
- token parse failure -> walker
- manifest epoch mismatch -> walker + `marker_fallback` metric
- sampled drift mismatch -> walker + alert
- high fallback/error budget burn -> disable index path for affected bucket/range

### 6.7 rollout / rollback closure package

Rollout phases:

| Phase | Scope | Success condition | Abort condition |
| --- | --- | --- | --- |
| 0 | local/offline validation | continuity pass, unit/static pass | duplicate/missing in local smoke |
| A | shadow benchmark on production-shaped bucket | two consecutive windows with continuity pass and stable latency | any continuity failure |
| B | canary bucket or prefix range | error budget within guardrail, fallback rate below threshold | p99 +20%, error +10%, fallback spike |
| C | broadened prefix families | same as canary for two windows | any regression repeats in two windows |
| D | wider rollout | observability remains stable | operator rollback trigger |

Explicit rollback conditions:
- duplicate objects across pages
- missing objects after truncation / continuation transition
- p99 latency regression `> +20%`
- error-rate regression `> +10%`
- fallback rate above the planned threshold
- sampled index drift mismatch

Rollback actions:
- quorum-only experiment: set `RUSTFS_LIST_OBJECTS_QUORUM=strict`
- future index-assisted planner: disable index path and force walker
- token-family regression: stop emitting proposed v3 index tokens and continue accepting legacy / v2 parser branches only

Closure interpretation for `#787`:
- the benchmark evidence is now attached and complete;
- the outcome is negative for quorum-only rollout;
- the design/rollout package now supports moving the real mitigation focus to index-backed listing.

## 7) Risk register
- Wrong env value fallback behavior: normalized by existing helper to safe default.
- Quorum drift across environments: explicit rollout control via env variable in deployment chart.
- Unexpected compatibility concerns: no client-facing protocol change in this phase.

## 8) Rollback plan
- Set `RUSTFS_LIST_OBJECTS_QUORUM=strict` at runtime/restart.
- If needed, revert this commit only.

## 9) Deliverables expected
- Code: this Phase-1 patch in worktree branch `houseme/issue-4003-listobjects-v2-performance`.
- Docs: English and Chinese documents (this plan and Chinese counterpart).
- Tracking: backlog issues `#785`, `#786`, `#787` under parent `#788`.
