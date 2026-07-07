# ECStore Validation Suite Design

This document defines the validation suite RustFS should use before claiming
ECStore erasure-coding correctness, durability, and fault-resilience coverage.
It is a suite design, not a claim that all tests already exist.

## Goal

Provide one command that runs the full ECStore confidence suite and produces a
single pass/fail result plus artifacts. The command should exercise both:

- white-box invariants in `rustfs-ecstore`, `rustfs-filemeta`, and related
  helpers;
- black-box S3 and admin behavior against real single-node and distributed
  erasure deployments.

The suite can reduce release risk; it cannot prove the absence of all defects.
It must therefore combine deterministic matrices, negative tests, fuzz/corpus
tests, chaos tests, and explicit artifact review.

## Proposed Entry Point

Use the top-level runner for repeatable local, CI, and release checks:

```bash
scripts/run_ecstore_validation_suite.sh --profile full
```

Profiles:

| Profile | Purpose | Expected cost |
| --- | --- | --- |
| `quick` | PR smoke for EC logic and existing single-node reliability tests. | minutes |
| `full` | Release gate: white-box, e2e, chaos, S3 compatibility subset, coverage. | hours |
| `destructive` | Manual/nightly gate: distributed 4-node/16-disk, crash/restart, rebalance/decommission fault injection. | hours+ |
| `fuzz` | Malformed metadata/RPC/corpus fuzzing with fixed seed output. | bounded by budget |

The current runner wires the existing high-signal checks. The matrix below
still tracks required follow-up coverage before `full` can be treated as a
complete release gate.

The runner writes artifacts under
`target/ecstore-validation/<timestamp>/`:

- command transcript and environment;
- randomized seeds;
- object manifests with SHA256;
- per-disk layout snapshots before and after mutation;
- server logs;
- junit/nextest output;
- `blackbox-matrix.tsv` with selected black-box and fixture gates;
- coverage report;
- failure reproducer instructions.

## Acceptance Rules

The suite passes only when all selected profile commands pass and every
scenario asserts both API-visible behavior and on-disk state where applicable.

Required fail-closed rules:

- never return corrupted object bytes;
- never silently accept forged or split-brain metadata;
- never downgrade write quorum to read quorum;
- never leave a mixed old/new object after partial commit;
- never panic on malformed EC metadata;
- return typed errors or quorum failures for invalid states.

Unit-test coverage is a hard release-gate input: `rustfs-ecstore` unit line
coverage for the EC-critical scope must be at least 95%, with 100% as the
target for EC read, write, decode, heal, metadata quorum, and rollback paths.
A lower threshold is only acceptable for a temporary, explicitly documented
exception tied to missing testability or unreachable code. Full-crate coverage
is still reported as an observation metric, but it must not hide EC regressions
behind unrelated modules.

Tests must not pass by only checking constants, helper calls, deleted branches,
or implementation details. Every test needs a reader-facing or storage-state
assertion.

Fixture-backed checks are optional for local smoke runs, but explicit in the
artifact stream. `--require-fixtures` turns missing legacy or MinIO generated
fixtures into an early `ecstore-fixture-gate` failure before expensive black-box
steps run. The MinIO generated fixture gate requires both
`RUSTFS_MINIO_FIXTURE_ROOT` and `RUSTFS_MINIO_STATIC_KMS_KEY_B64`.

## White-Box Matrix

### Erasure Algorithm

Target files:

- `crates/ecstore/src/erasure/coding/erasure.rs`
- `crates/ecstore/src/erasure/coding/encode.rs`
- `crates/ecstore/src/erasure/coding/decode.rs`
- `crates/ecstore/src/erasure/coding/decode_reader.rs`
- `crates/ecstore/src/erasure/codec/bridge.rs`

Coverage:

| Area | Scenarios | Assertions |
| --- | --- | --- |
| Shard geometry | legacy/current shard-size formulas; lengths `0`, `1`, `block-1`, `block`, `block+1`, multi-block tail | no divide-by-zero; shard/file/range offsets match expected |
| Encode/decode | `(data, parity)` sets `2+2`, `4+2`, `8+8`; random payloads; missing shards up to parity | reconstructed data equals original |
| Negative reconstruction | missing shards above parity; inconsistent shard lengths; corrupt surplus parity | typed error, no partial success |
| Source verification | missing data shard plus extra parity source | rebuilt parity must match source parity |
| Legacy compatibility | old shard formula and legacy checksum data | legacy files decode and heal correctly |
| Streaming decode | legacy engine vs RustFS codec engine on same stripe stream | bytes and errors are equivalent |
| Range output | head/middle/tail/suffix; cross-block and final-short-stripe ranges | exact byte range, no over-read/under-read |

Add property tests with fixed replay seeds for payload, range, and missing-shard
selection.

### Bitrot and Reader Alignment

Target files:

- `crates/ecstore/src/erasure/coding/bitrot.rs`
- `crates/ecstore/src/erasure/coding/decode.rs`
- `crates/ecstore/src/set_disk/core/io_primitives.rs`
- `crates/ecstore/src/set_disk/shard_source.rs`

Coverage:

| Area | Scenarios | Assertions |
| --- | --- | --- |
| Hash framing | valid hash+data; wrong hash; truncated hash; truncated data | invalid data never succeeds |
| Short shard | short read under normal hash, `skip_verify`, and hash-none | `UnexpectedEof` or equivalent typed error |
| Lockstep reads | mid-stream data shard failure; pending/timeout reader; final short stripe | each live reader advances exactly one stripe; failed reader retires |
| Adaptive reads | hedged parity fallback and timeout retirement | no shard desync; reconstructed bytes match original |
| Shard source order | out-of-order read completion and missing slots | slots resolve by shard index |
| Deferred readers | data-blocks-first setup opens deferred parity at correct offset | parity fallback uses aligned data |

Use instrumented readers that record shard index, stripe index, read count,
offset, and retirement reason.

### Metadata, Quorum, and Commit Atomicity

Target files:

- `crates/filemeta/src/filemeta/version.rs`
- `crates/ecstore/src/set_disk/read.rs`
- `crates/ecstore/src/set_disk/metadata.rs`
- `crates/ecstore/src/set_disk/ops/object.rs`
- `crates/ecstore/src/set_disk/core/io_primitives.rs`
- `crates/ecstore/src/disk/local.rs`

Coverage:

| Area | Scenarios | Assertions |
| --- | --- | --- |
| Metadata tamper | same `version_id`/`mod_time`, divergent data dir, parts, ETag, size, checksum, inline flag, erasure distribution | previous committed version or read quorum error; no arbitrary latest |
| Early stop | valid quorum, stale quorum, corrupt trailing disks, slow trailing disks | early-stop only on safe identity |
| Quorum downgrade | read quorum vs write quorum; delete marker quorum; version-not-found quorum | no mutation below write quorum |
| Rename atomicity | failure before data rename, after data rename, after metadata rename, cleanup failure | object is old or new; never mixed |
| Rollback | failed commit quorum and stale temp data | rollback preserves old metadata/data |
| Malformed metadata | oversized lengths, bad CRC, invalid versions, invalid UUID/timestamp/enum, huge parts | bounded memory; typed error; no panic |

Fault injection should be explicit and deterministic, preferably through local
disk mocks for unit tests and process-level disk manipulation for e2e tests.

## Black-Box Matrix

Use `crates/e2e_test` for real S3/admin behavior and extend
`crates/e2e_test/src/chaos.rs` rather than duplicating ad hoc helpers.

The current runner emits `blackbox-matrix.tsv` for every invocation. It is a
machine-readable manifest of selected black-box scenarios, their commands,
required fixture environment, and whether each row is enabled, disabled, or
missing an optional/required fixture.

Current runner rows:

| Profile | Scenario | Gate | Fixture env |
| --- | --- | --- | --- |
| `quick` | single-node disk fault read/write | e2e black box | none |
| `quick` | degraded erasure disk rebuild | e2e black box | none |
| `quick` | namespace lock quorum under EC ops | e2e black box | none |
| `full` | legacy bitrot read fixture restore | fixture black box | `RUSTFS_LEGACY_TEST_ROOT`, `RUSTFS_LEGACY_TEST_DISK` |
| `full` | MinIO generated encrypted read and negative restore fixture | fixture black box | `RUSTFS_MINIO_FIXTURE_ROOT`, `RUSTFS_MINIO_STATIC_KMS_KEY_B64` |
| `full` | S3 multipart/range/versioning/delete subset | S3 black box | none |
| `destructive` | distributed cluster concurrency | e2e black box | none |
| `destructive` | stale multipart cleanup cluster | e2e black box | none |
| `destructive` | delete marker migration semantics | e2e black box | none |

### Single-Node 4-Disk EC

| Scenario | Required assertions |
| --- | --- |
| baseline PUT/GET/HEAD/List for tiny, inline, block-boundary, multi-block, multipart objects | SHA256 manifest matches; metadata is consistent on all disks |
| one disk offline during read | existing objects readable; no corrupted bytes |
| one disk offline during write | write succeeds only when write quorum holds; restored disk is healed |
| above-parity disk loss | GET/PUT fails with quorum error; no partial bytes accepted |
| corrupt data shard and parity shard | GET returns original bytes or fails closed; read-repair/heal restores |
| corrupt inline `xl.meta` | fail closed or heal; no forged inline data |
| range read with offline/corrupt shard | exact range bytes; invalid ranges produce expected S3 errors |
| multipart part resend and concurrent same-part writes | final complete object matches chosen committed parts |
| crash during multipart complete/put/delete | after restart only old or new full version is visible |

Existing anchors:

- `crates/e2e_test/src/reliability_disk_fault_test.rs`
- `crates/e2e_test/src/heal_erasure_disk_rebuild_test.rs`
- `crates/e2e_test/src/chaos.rs`

### Distributed 4-Node / 16-Disk EC

| Scenario | Required assertions |
| --- | --- |
| node/disk outage while reading large objects | no EOF/truncation; SHA256 manifest matches |
| write while one remote node is down | write follows quorum; later heal reconstructs remote disk |
| remote shard bitrot | degraded read uses clean shards; no bad bytes |
| concurrent GET/PUT/DELETE/List on same key | no 500 for expected conflicts; no dirty reads |
| range GET matrix for large objects | sequential and parallel ranges match full-object hash |
| internode timeout/slow disk | typed error or fallback; no desync |

This layer is mandatory because single-process unit tests cannot prove RPC,
HTTP/2, timeout, and distributed quorum behavior.

### Versioning, Delete Markers, and Migration

| Scenario | Required assertions |
| --- | --- |
| latest delete marker | GET/HEAD/ListObjectVersions match S3 semantics |
| explicit `versionId` for old versions | exact old bytes and metadata |
| suspended/null version | no version ordering regression |
| delete marker during heal/rebalance/decommission | marker visibility and history are preserved |
| orphan directory cleanup | real objects are not purged; tombstones behave correctly |

### Heal, Rebalance, and Decommission

| Scenario | Required assertions |
| --- | --- |
| auto heal and admin deep heal | data hash unchanged; `xl.meta` and format data rebuilt |
| heal interruption/restart | idempotent recovery; no dangling temp objects |
| two-pool rebalance with versioned/multipart objects | source and target pools have consistent versions |
| decommission cancel/restart/finalize | target readable; source cleanup safe |
| rebalance/decommission with node outage | progress resumes; no duplicate or missing versions |

Existing scripts under `scripts/test/decommission_*.sh` should be wrapped into
the `destructive` profile only after they emit machine-readable pass/fail
artifacts.

## S3 Compatibility and Large Object Gates

The full profile should include a targeted S3 compatibility subset, not the
entire compatibility suite by default:

```bash
TESTEXPR="multipart or range or versioning or delete" \
DEPLOY_MODE=build \
MAXFAIL=0 \
./scripts/s3-tests/run.sh
```

Large object gates:

- `scripts/run_get_codec_streaming_smoke.sh` for legacy/codec GET parity;
- `scripts/run_gt1g_get_http_matrix.sh` for sequential and parallel range GET;
- `scripts/run_gt1g_multipart_put_matrix.sh` for multipart PUT paths.

These should be artifact-producing optional stages in `full` or `destructive`
profiles, not hidden local-only commands.

## Fuzz and Corpus Gates

Add bounded fuzz/corpus tests for:

- `xl.meta` MessagePack and legacy filemeta versions;
- protobuf/RPC payload decoding;
- checksum and bitrot headers;
- range offset/length overflow;
- object names with path traversal, encoded separators, and symlink components;
- huge inline metadata and huge part counts.

Each corpus failure must save the input bytes and the minimized reproducer under
the suite artifact directory.

## Coverage Snapshot Target

The runner enforces unit line coverage for `rustfs-ecstore` in `full` and
`destructive` profiles. The default threshold is 95%, and the target remains
100% for EC-critical paths:

```bash
scripts/run_ecstore_validation_suite.sh --profile full --unit-coverage-min 95
scripts/run_ecstore_validation_suite.sh --profile full --unit-coverage-min 100
scripts/run_ecstore_validation_suite.sh --profile full --unit-coverage-scope crate
```

The default hard gate scope is `ec-critical`, covering:

- `crates/ecstore/src/erasure/**`
- `crates/ecstore/src/set_disk/read.rs`
- `crates/ecstore/src/set_disk/shard_source.rs`
- `crates/ecstore/src/set_disk/metadata.rs`
- `crates/ecstore/src/set_disk/ops/object.rs`
- `crates/ecstore/src/set_disk/core/io_primitives.rs`
- `crates/ecstore/src/disk/local.rs`

Minimum release-gate target:

- `cargo-llvm-cov` must be installed unless `--skip-coverage` is explicitly set;
- `rustfs-ecstore` EC-critical unit line coverage is at least 95%;
- EC read/write/decode/heal/quorum/rollback code should trend toward 100%;
- full-crate unit line coverage is recorded for visibility but is not the EC
  hard gate;
- all HIGH rows in this document have positive and negative tests;
- changed EC/read/write/heal lines are covered;
- branch coverage is reviewed for reconstruction, quorum, and error paths;
- uncovered branches are either intentionally unreachable or tracked.

The `quick` profile also runs
`cargo test -p rustfs-ecstore --lib -- --test-threads=1` so the white-box smoke
includes all current `rustfs-ecstore` library unit tests without parallel test
context cross-talk, not only focused EC filters.

Coverage artifacts:

- `target/ecstore-validation/<timestamp>/coverage/ecstore/lcov.info`
- `target/ecstore-validation/<timestamp>/coverage/ecstore/summary.tsv`
- `target/ecstore-validation/<timestamp>/coverage/ecstore/files.tsv`

Local validation on 2026-07-07 found that the current suite is not yet at the
target: full-crate unit line coverage was 69.32%; the EC-critical scope was
84.32% after the first negative read/write/recovery additions. The lowest
EC-critical files were `set_disk/ops/object.rs`, `io_primitives.rs`,
`disk/local.rs`, `bitrot.rs`, and `encode.rs`. These are gaps to close before
the default 95% gate can pass.

## Initial Command Set

Use the runner first:

```bash
scripts/run_ecstore_validation_suite.sh --profile quick
scripts/run_ecstore_validation_suite.sh --profile full
scripts/run_ecstore_validation_suite.sh --profile destructive
scripts/run_ecstore_validation_suite.sh --profile fuzz
```

The split-run equivalent for the quick profile is:

```bash
cargo test -p rustfs-filemeta --lib
cargo test -p rustfs-ecstore --lib erasure
cargo test -p rustfs-ecstore --lib set_disk::read
cargo test -p rustfs-ecstore --lib set_disk::core::io_primitives
cargo test -p rustfs-ecstore --lib set_disk::tests::test_rename_data_quorum_failure_rolls_back_destination_object
cargo test -p rustfs-ecstore --lib disk::local
cargo test -p rustfs-ecstore --lib -- --test-threads=1
cargo test --package e2e_test reliability_disk_fault_test -- --nocapture
cargo test --package e2e_test heal_erasure_disk_rebuild_test -- --nocapture
cargo test --package e2e_test namespace_lock_quorum_test -- --nocapture
```

Fixture-backed tests should run when the fixture path is present:

```bash
cargo test -p rustfs-ecstore --test legacy_bitrot_read_test -- --nocapture
cargo test -p rustfs-ecstore --features rio-v2 --test minio_generated_read_test -- --ignored --nocapture
```

## Multi-Expert Adversarial Review Summary

Three independent read-only reviews were run for this design:

| Reviewer | Main challenge | Resulting requirement |
| --- | --- | --- |
| Algorithm correctness | A single run only samples one timing/layout/hash combination. | matrix/property tests plus instrumented reader alignment checks |
| Black-box reliability | Existing tests miss distributed, shard-desync, range+fault, and migration-fault combinations. | add 4-node/16-disk and destructive profiles |
| Security and fault review | Metadata tamper, quorum downgrade, bitrot bypass, and rename atomicity must be end-to-end. | HIGH matrix rows block completion claims |

Current verdict: design accepted as a target. The runner exists and captures
artifacts; implementation remains incomplete until the missing matrix rows have
artifact-backed evidence.
