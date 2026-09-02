# ECStore Validation Suite

**Use this when:** you run or extend `scripts/run_ecstore_validation_suite.sh`, or add an erasure-coding test and need the scenario row it must satisfy.
**Source of truth:** `scripts/run_ecstore_validation_suite.sh` (profiles, flags, step commands, coverage scope, default thresholds); `docs/architecture/erasure-coding.md` (the invariants the rows enforce).

## Runner contract

```bash
scripts/run_ecstore_validation_suite.sh --profile <quick|full|destructive|fuzz> \
  [--out-dir <path>] [--skip-e2e] [--skip-s3-tests] [--skip-coverage] [--require-fixtures] \
  [--unit-coverage-min <percent>] [--unit-coverage-scope <ec-critical|crate>] [--dry-run]
```

The runner is a local and release-validation tool; no CI workflow invokes it.

| Profile | Steps (each profile includes the one above it) | Cost |
|---|---|---|
| `quick` | `rustfs-filemeta` lib tests; focused `rustfs-ecstore` lib filters (`erasure`, `set_disk::read`, `set_disk::core::io_primitives`, the rename-rollback test, `disk::local`); the two isolated global-state tests named at the top of the runner; the whole `rustfs-ecstore --lib` with `--test-threads=1`; e2e `reliability_disk_fault_test`, `heal_erasure_disk_rebuild_test`, `namespace_lock_quorum_test` | minutes |
| `full` | fixture gate; legacy-bitrot and MinIO generated-read fixture tests; s3-tests subset `TESTEXPR="multipart or range or versioning or delete"` with `DEPLOY_MODE=build MAXFAIL=0`; unit coverage gate | hours |
| `destructive` | `disk::local::test::crash_consistency` (power loss at each pre-commit step; object reopens as old or new, never mixed); e2e `cluster_concurrency_test`, `stale_multipart_cleanup_cluster_test`, `delete_marker_migration_semantics_test` | hours+ |
| `fuzz` | `scripts/fuzz/run.sh` bounded by `MAX_TOTAL_TIME` (default in the runner) | bounded |

Artifacts under `<out-dir>` (default `target/ecstore-validation/<timestamp>/`):

| File | Content |
|---|---|
| `run-metadata.env` | profile, flags, thresholds, environment |
| `summary.tsv` | one row per step: pass / fail / skip with reason |
| `blackbox-matrix.tsv` | every black-box and fixture row with command, fixture env, and status `enabled` / `disabled` / `missing-optional` / `missing-required`; written on every invocation regardless of profile |
| `logs/*.log` | per-step transcripts |
| `coverage/ecstore/lcov.info`, `summary.tsv`, `files.tsv` | coverage export, gate summary, per-file table (`full` and `destructive`) |

Fixture rows:

| Row | Profile | Fixture env |
|---|---|---|
| legacy bitrot read (`crates/ecstore/tests/legacy_bitrot_read_test.rs`) | `full` | `RUSTFS_LEGACY_TEST_ROOT`, `RUSTFS_LEGACY_TEST_DISK` |
| MinIO generated encrypted read and negative restore (`storage::minio_generated_read_test` in `rustfs/src/storage/mod.rs`, `--features rio-v2`, `--ignored`) | `full` | `RUSTFS_MINIO_FIXTURE_ROOT`, `RUSTFS_MINIO_STATIC_KMS_KEY_B64` |

A missing fixture is recorded as a `missing-optional` skip. `--require-fixtures` turns it into an early `ecstore-fixture-gate` failure before the expensive black-box steps run.

## Acceptance rules

A run passes only when every selected step passes. Every scenario asserts API-visible behaviour and, where applicable, on-disk state; a test that only checks constants, helper calls, deleted branches, or implementation details does not satisfy a row.

Fail-closed invariants every row enforces:

- never return corrupted object bytes;
- never silently accept forged or split-brain metadata;
- never downgrade write quorum to read quorum;
- never leave a mixed old/new object after a partial commit;
- never panic on malformed EC metadata;
- return typed errors or quorum failures for invalid states.

Fault injection is explicit and deterministic: local disk mocks for unit tests, process-level disk manipulation (`crates/e2e_test/src/chaos.rs`) for e2e tests. Property tests replay a fixed seed for payload, range, and missing-shard selection.

### Coverage gate

`full` and `destructive` run `cargo llvm-cov -p rustfs-ecstore --lib` and fail when line coverage of the gate scope is below `--unit-coverage-min`. The default minimum and the 100% target for EC read, write, decode, heal, metadata-quorum, and rollback paths are the `UNIT_COVERAGE_*` constants at the top of the runner. `cargo-llvm-cov` must be installed unless `--skip-coverage` is passed explicitly. The default scope `ec-critical` is:

- `crates/ecstore/src/erasure/**`
- `crates/ecstore/src/set_disk/read.rs`
- `crates/ecstore/src/set_disk/shard_source.rs`
- `crates/ecstore/src/set_disk/metadata.rs`
- `crates/ecstore/src/set_disk/ops/object.rs`
- `crates/ecstore/src/set_disk/core/io_primitives.rs`
- `crates/ecstore/src/disk/local.rs`

`--unit-coverage-scope crate` measures the whole crate instead; that number is an observation metric and must not hide EC regressions behind unrelated modules. Lowering the minimum requires a documented exception tied to missing testability or unreachable code. Uncovered branches in reconstruction, quorum, and error paths are either intentionally unreachable or tracked.

## White-box scenario matrix

Each row is a scenario the unit layer covers; a new EC unit test names the row it satisfies.

### Erasure algorithm (`crates/ecstore/src/erasure/`)

| Area | Scenarios | Assertions |
| --- | --- | --- |
| Shard geometry | legacy/current shard-size formulas; lengths `0`, `1`, `block-1`, `block`, `block+1`, multi-block tail | no divide-by-zero; shard/file/range offsets match expected |
| Encode/decode | `(data, parity)` sets `2+2`, `4+2`, `8+8`; random payloads; missing shards up to parity | reconstructed data equals original |
| Negative reconstruction | missing shards above parity; inconsistent shard lengths; corrupt surplus parity | typed error, no partial success |
| Source verification | missing data shard plus extra parity source | rebuilt parity matches source parity |
| Legacy compatibility | old shard formula and legacy checksum data | legacy files decode and heal correctly |
| Streaming decode | legacy engine vs RustFS codec engine on the same stripe stream | bytes and errors are equivalent |
| Range output | head/middle/tail/suffix; cross-block and final-short-stripe ranges | exact byte range, no over-read or under-read |

### Bitrot and reader alignment (`erasure/coding/bitrot.rs`, `decode.rs`, `set_disk/core/io_primitives.rs`, `set_disk/shard_source.rs`)

| Area | Scenarios | Assertions |
| --- | --- | --- |
| Hash framing | valid hash+data; wrong hash; truncated hash; truncated data | invalid data never succeeds |
| Short shard | short read under normal hash, `skip_verify`, and hash-none | `UnexpectedEof` or equivalent typed error |
| Lockstep reads | mid-stream data shard failure; pending/timeout reader; final short stripe | each live reader advances exactly one stripe; failed reader retires |
| Adaptive reads | hedged parity fallback and timeout retirement | no shard desync; reconstructed bytes match original |
| Shard source order | out-of-order read completion and missing slots | slots resolve by shard index |
| Deferred readers | data-blocks-first setup opens deferred parity at the correct offset | parity fallback uses aligned data |

Instrumented readers record shard index, stripe index, read count, offset, and retirement reason.

### Metadata, quorum, and commit atomicity (`crates/filemeta/src/filemeta/version.rs`, `set_disk/read.rs`, `set_disk/metadata.rs`, `set_disk/ops/object.rs`, `disk/local.rs`)

| Area | Scenarios | Assertions |
| --- | --- | --- |
| Metadata tamper | same `version_id`/`mod_time`, divergent data dir, parts, ETag, size, checksum, inline flag, erasure distribution | previous committed version or read-quorum error; no arbitrary latest |
| Early stop | valid quorum, stale quorum, corrupt trailing disks, slow trailing disks | early-stop only on safe identity |
| Quorum downgrade | read quorum vs write quorum; delete-marker quorum; version-not-found quorum | no mutation below write quorum |
| Rename atomicity | failure before data rename, after data rename, after metadata rename, cleanup failure | object is old or new; never mixed |
| Rollback | failed commit quorum and stale temp data | rollback preserves old metadata and data |
| Malformed metadata | oversized lengths, bad CRC, invalid versions, invalid UUID/timestamp/enum, huge parts | bounded memory; typed error; no panic |

## Black-box scenario matrix

Real S3 and admin behaviour runs through `crates/e2e_test`. Extend `crates/e2e_test/src/chaos.rs` rather than adding ad hoc fault helpers.

### Single-node 4-disk EC (`reliability_disk_fault_test.rs`, `heal_erasure_disk_rebuild_test.rs`, `chaos.rs`)

| Scenario | Required assertions |
| --- | --- |
| baseline PUT/GET/HEAD/List for tiny, inline, block-boundary, multi-block, multipart objects | SHA256 manifest matches; metadata is consistent on all disks |
| one disk offline during read | existing objects readable; no corrupted bytes |
| one disk offline during write | write succeeds only when write quorum holds; restored disk is healed |
| above-parity disk loss | GET/PUT fails with a quorum error; no partial bytes accepted |
| corrupt data shard and parity shard | GET returns original bytes or fails closed; read-repair/heal restores |
| corrupt inline `xl.meta` | fail closed or heal; no forged inline data |
| range read with offline/corrupt shard | exact range bytes; invalid ranges produce the expected S3 errors |
| multipart part resend and concurrent same-part writes | final object matches the chosen committed parts |
| crash during multipart complete/put/delete | after restart only the old or the new full version is visible |

### Distributed 4-node / 16-disk EC (`cluster_concurrency_test.rs`, `namespace_lock_quorum_test.rs`, `stale_multipart_cleanup_cluster_test.rs`)

Single-process unit tests cannot prove RPC, HTTP/2, timeout, or distributed-quorum behaviour, so this layer is mandatory.

| Scenario | Required assertions |
| --- | --- |
| node/disk outage while reading large objects | no EOF/truncation; SHA256 manifest matches |
| write while one remote node is down | write follows quorum; later heal reconstructs the remote disk |
| remote shard bitrot | degraded read uses clean shards; no bad bytes |
| concurrent GET/PUT/DELETE/List on the same key | no 500 for expected conflicts; no dirty reads |
| range GET matrix for large objects | sequential and parallel ranges match the full-object hash |
| internode timeout / slow disk | typed error or fallback; no desync |

### Versioning, delete markers, and migration (`delete_marker_migration_semantics_test.rs`)

| Scenario | Required assertions |
| --- | --- |
| latest delete marker | GET/HEAD/ListObjectVersions match S3 semantics |
| explicit `versionId` for old versions | exact old bytes and metadata |
| suspended/null version | no version-ordering regression |
| delete marker during heal/rebalance/decommission | marker visibility and history are preserved |
| orphan directory cleanup | real objects are not purged; tombstones behave correctly |

### Heal, rebalance, and decommission

| Scenario | Required assertions |
| --- | --- |
| auto heal and admin deep heal | data hash unchanged; `xl.meta` and format data rebuilt |
| heal interruption/restart | idempotent recovery; no dangling temp objects |
| two-pool rebalance with versioned/multipart objects | source and target pools have consistent versions |
| decommission cancel/restart/finalize | target readable; source cleanup safe |
| rebalance/decommission with node outage | progress resumes; no duplicate or missing versions |

`scripts/test/decommission_*.sh` cover parts of this table but are not runner steps because they emit no machine-readable pass/fail artifact.

## Large-object and fuzz gates

Stand-alone harnesses indexed in `scripts/README.md`, not runner steps: `scripts/run_get_codec_streaming_smoke.sh` (legacy vs codec GET parity), `scripts/run_gt1g_get_http_matrix.sh` (sequential and parallel range GET above 1 GiB), `scripts/run_gt1g_multipart_put_matrix.sh` (multipart PUT above 1 GiB).

The `fuzz` profile delegates to `scripts/fuzz/run.sh`; targets, corpus rules, and crash-reproducer locations are in `fuzz/README.md`. Malformed-storage-input surfaces the corpus must cover: `xl.meta` MessagePack and legacy filemeta versions, RPC payload decoding, checksum and bitrot headers, range offset/length overflow, object names with path traversal or encoded separators, huge inline metadata and part counts.
