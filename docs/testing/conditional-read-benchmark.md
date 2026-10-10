# Conditional object-read benchmark

**Use this when:** validating the performance of metadata-based conditional GET
changes or reproducing a paired application-read regression experiment.

Conditional GET acceptance needs paired performance evidence for ordinary reads,
condition hits, and condition misses. Saving body work on a 304 does not establish
that other reads are unaffected. Keep performance acceptance pending if resource
isolation, baseline stability, or regression budgets fail.

The manually invoked `conditional_read_paired_benchmark` drives the actual
`DefaultObjectUsecase` against four local EC disks. It consumes and verifies the
entire successful body, validates 304 results, and samples existing read-lock
hold histograms. It covers 4 KiB inline and 1.3 MB sharded objects, body cache off
and warmed, and a 20 ms delay on one metadata disk. It excludes network ingress,
authentication, external tiers, cold-fill contention, and distributed capacity.

The test-util-only metadata guard bypasses metadata caching in the dedicated
`conditional-bench` bucket and permits the existing delay injector to affect
metadata-only prepared reads. This prevents cache hits or `read_data=false`
from silently hiding the slow-disk scenario. The guard has no effect in ordinary
builds or outside its bucket, and ownership drop restores the default policy.
Both compared revisions must include these identical benchmark controls.

## Build paired artifacts

Use a clean detached worktree at the full baseline commit, a separate candidate
worktree, and matching stable toolchain, features, and build profile. The adapter
resolves every source anchor before writing. It installs only the test harness,
test-util controls, and their facade exports; it does not install the conditional
read implementation on the baseline. Review its diff before building.

```bash
python3 scripts/prepare_conditional_read_baseline.py \
  --baseline-worktree "$BASELINE_WORKTREE" --revision "$BASELINE_REVISION"
```

In each worktree, format the adapter and build the same artifact:

```bash
RUSTUP_TOOLCHAIN=stable cargo fmt --all
RUSTUP_TOOLCHAIN=stable cargo test --locked -p rustfs --lib \
  --no-default-features --features rio-v2 \
  conditional_read_paired_benchmark --no-run
```

Save each executable printed by Cargo before another build overwrites it. For
deployment acceptance, use matching optimized profiles and an isolated target
environment. A test-profile run is a local application regression experiment,
not a production throughput estimate.

## Run and interpret

Stop concurrent builds and other workloads on the measurement resources. The
runner refuses concurrent Cargo/rustc processes or high host load. It executes
fresh processes serially in A1–B1–B2–A2 order, removes ambient RustFS experimental
settings and proxies, uses independent temporary disks, and records binary,
revision, harness, and profile provenance beside raw logs.

Executable snapshots are read-only and their hashes are rechecked before and
after every case. Cargo/rustc activity is sampled during each process; host load
and isolation are checked again on exit, including the last A2 case. These checks
do not replace exclusive resource ownership for formal deployment acceptance.

```bash
python3 scripts/run_conditional_read_abba.py \
  --baseline "$BASELINE_BINARY" --candidate "$CANDIDATE_BINARY" \
  --baseline-revision "$BASELINE_REVISION" \
  --candidate-revision "$CANDIDATE_REVISION" \
  --harness rustfs/src/app/object/conditional_read_bench.rs \
  --profile test --out-dir "$RESULT_DIR"
```

The default is 1000 requests per case at concurrency 8. Each case records
throughput, p95/p99 latency, read-lock p95/p99, and a zero metadata-cache-hit
assertion. Body-cache-on cases are warmed; they do not measure cold-fill misses.
Inline shard bytes can be carried by an `xl.meta` read even when no separate
body reader, bitrot verification, or decode is needed.

Budgets are fixed before the run:

- Baseline drift: throughput within 10%; latency and lock percentiles within
  15%, with a 0.25 ms absolute noise floor.
- Candidate regression: throughput loss at most 5%; latency and lock percentile
  increases at most 10%, with a 0.25 ms absolute noise floor.

A drift failure produces no candidate attribution. After a stable baseline, the
runner compares geometric means of the paired legs. Any workload over budget
keeps acceptance pending; improving 304 cannot compensate for an ordinary-read
or condition-miss regression. Preserve raw logs and investigate before rerunning;
do not relax budgets after seeing the results.

Run `python3 scripts/test_conditional_read_abba.py` to verify the acceptance
parser and drift/regression gates.
