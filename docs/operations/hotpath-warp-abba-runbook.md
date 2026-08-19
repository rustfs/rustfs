# Hotpath warp ABBA validation runbook

This runbook describes how to collect formal Linux or production-cluster
evidence for hotpath performance changes. Use it when a short local A/B smoke
run is too noisy to decide whether a regression is real.

The ABBA runner executes each workload and drive-sync cell as:

```text
A1 baseline -> B1 candidate -> B2 candidate -> A2 baseline
```

`B1` and `B2` are compared with `A1` to measure the candidate delta. `A2` is
also compared with `A1` to measure baseline drift. Treat a candidate regression
as actionable only when the `A2` drift is passing or materially smaller than
the `B1` and `B2` delta for the same workload.

## Scope

Use this runbook for hotpath profiling and performance validation of RustFS
object I/O changes, especially when CPU, memory allocation, lock/channel wait
time, request throughput, or tail latency is the review question.

The script validates the same workload matrix as the hotpath warp A/B gate:

| Workload | mode | size |
| --- | --- | --- |
| `put-4kib` | put | 4KiB |
| `put-4mib` | put | 4MiB |
| `get-4kib` | get | 4KiB |
| `get-4mib` | get | 4MiB |
| `get-10mib` | get | 10MiB |
| `mixed-256k` | mixed | 256KiB |

Each workload runs with `RUSTFS_DRIVE_SYNC_ENABLE=true` and
`RUSTFS_DRIVE_SYNC_ENABLE=false`, so a full ABBA pass produces 48 measurement
cells: 6 workloads x 2 drive-sync modes x 4 ABBA legs.

## Prerequisites

Run the formal pass on Linux, not on a laptop smoke environment.

Required tools on the bench host:

- `bash`, `curl`, `git`, and core GNU userland.
- `warp` on `PATH`, or pass `--warp-bin`.
- Two RustFS Linux binaries: one baseline and one candidate.
- Enough isolated disks or directories for the local runner, or an externally
  managed RustFS cluster for production-like validation.
- Stable host telemetry collection such as `pidstat`, `mpstat`, `iostat`,
  `sar`, `perf`, `heaptrack`, or the platform's equivalent observability stack.

Cluster-mode requirements:

- A deploy hook that can replace the RustFS binary on every node.
- The hook must apply `RUSTFS_DRIVE_SYNC_ENABLE` for the current ABBA leg.
- The hook must restart RustFS and return only after the rollout command has
  been accepted. The ABBA script performs the HTTP readiness wait.
- The benchmark client should run outside the RustFS nodes when possible.
- Do not run against a production data set unless the workload bucket and test
  credentials are isolated and approved for destructive benchmark traffic.

## Build the binaries

Build the baseline from the comparison commit, usually `origin/main` or the
previous accepted release:

```bash
git fetch origin main
git switch --detach origin/main
cargo build --release -p rustfs --bins
cp target/release/rustfs /tmp/rustfs-baseline
```

Build the candidate from the PR commit:

```bash
git switch <candidate-branch>
cargo build --release -p rustfs --bins
cp target/release/rustfs /tmp/rustfs-candidate
```

For cross-compiled cluster binaries, keep both outputs on the bench host and
make the deploy hook copy the selected binary to the cluster. The ABBA runner
passes the selected binary path through `HOTPATH_ABBA_BINARY`.

## Local Linux runner

Use local mode for a dedicated Linux runner with disposable data paths. This is
not a substitute for a production-like cluster, but it is useful before spending
cluster time.

```bash
scripts/run_hotpath_warp_abba.sh \
  --baseline-bin /tmp/rustfs-baseline \
  --candidate-bin /tmp/rustfs-candidate \
  --address 127.0.0.1:9000 \
  --data-root /var/tmp/rustfs-hotpath-abba \
  --disks 4 \
  --duration 120s \
  --rounds 3 \
  --cooldown 30 \
  --concurrency 16 \
  --out-dir target/hotpath-abba/linux-local
```

The script starts and stops RustFS for each ABBA leg. The data root is
throwaway and should not contain important data.

## Production-like cluster runner

Use external mode when RustFS lifecycle is managed by ansible, systemd, a
cluster scheduler, or a dedicated deployment harness. In this mode the ABBA
script does not start RustFS directly; it calls `--deploy-hook` before each leg
and then waits for `http://<endpoint><health-path>`.

The deploy hook receives:

| Environment variable | Value |
| --- | --- |
| `HOTPATH_ABBA_LEG` | `A1`, `B1`, `B2`, or `A2` |
| `HOTPATH_ABBA_PHASE` | `baseline` or `candidate` |
| `HOTPATH_ABBA_BINARY` | selected baseline or candidate binary path |
| `HOTPATH_ABBA_DRIVE_SYNC` | `true` or `false` |

Example ansible-shaped command:

```bash
scripts/run_hotpath_warp_abba.sh \
  --baseline-bin /srv/rustfs-binaries/rustfs-baseline \
  --candidate-bin /srv/rustfs-binaries/rustfs-candidate \
  --endpoint rustfs-bench.example.internal:9000 \
  --deploy-hook '
    set -euo pipefail
    cd /srv/rustfs-ansible
    cp "${HOTPATH_ABBA_BINARY:?}" roles/rustfs/files/rustfs
    export RUSTFS_DRIVE_SYNC_ENABLE="${HOTPATH_ABBA_DRIVE_SYNC:?}"
    ansible-playbook -f 4 -l bench rustfs-manage.yml --tags stop
    ansible-playbook -f 4 -l bench rustfs-manage.yml --tags config
    ansible-playbook -f 4 -l bench rustfs-manage.yml --tags binary-copy
    ansible-playbook -f 4 -l bench rustfs-manage.yml --tags start
  ' \
  --duration 180s \
  --rounds 5 \
  --cooldown 45 \
  --concurrency 32 \
  --out-dir target/hotpath-abba/cluster-pr-XXXX
```

For formal evidence, prefer `--rounds 5` or higher when the cluster budget
allows it. The script enforces `--rounds >= 3`.

## CPU and memory evidence

ABBA warp output answers whether the candidate changed throughput or latency.
Collect host telemetry at the same time to explain why.

Recommended minimum:

```bash
mkdir -p target/hotpath-abba/cluster-pr-XXXX/telemetry

pidstat -durh 5 > target/hotpath-abba/cluster-pr-XXXX/telemetry/pidstat.txt &
PIDSTAT_PID=$!

mpstat 5 > target/hotpath-abba/cluster-pr-XXXX/telemetry/mpstat.txt &
MPSTAT_PID=$!

iostat -xz 5 > target/hotpath-abba/cluster-pr-XXXX/telemetry/iostat.txt &
IOSTAT_PID=$!
```

Stop the collectors after the ABBA script exits:

```bash
kill "$PIDSTAT_PID" "$MPSTAT_PID" "$IOSTAT_PID"
```

For deeper CPU attribution, run `perf record` around one representative
workload after the ABBA gate identifies a candidate regression or improvement:

```bash
perf record -F 99 -g -- sleep 180
perf report --stdio > target/hotpath-abba/cluster-pr-XXXX/telemetry/perf-report.txt
```

When using samply against an already-running RustFS service, attach through the
bounded helper instead of calling `samply record -p` directly:

```bash
scripts/run_samply_attach_window.sh \
  --pid "$RUSTFS_PID" \
  --duration-secs 180 \
  --output target/hotpath-abba/cluster-pr-XXXX/telemetry/samply-A1-get-4mib.json.gz
```

Run one attach window per ABBA leg or focused verification cell. After every
window, confirm that the `.json.gz` profile and `.syms.json` sidecar are
non-empty, that no `samply` process is still attached to the RustFS PID, and
that any temporary `perf_event_paranoid` change has been restored before the
next cell starts.

For allocation profiling, build the candidate with:

```bash
cargo build --release -p rustfs --bins --features hotpath-alloc
```

Then run the same ABBA command with that binary. Compare allocation-heavy
function sections only within the same build mode. Do not compare
`hotpath-alloc` binaries directly with default release binaries for throughput
acceptance, because allocation instrumentation intentionally changes what is
measured.

For CPU hotpath sections emitted by hotpath, build with:

```bash
cargo build --release -p rustfs --bins --features hotpath-cpu
```

Use the CPU-enabled report to explain hotspots after the default or plain
`hotpath` ABBA gate shows a real effect.

## Output layout

The ABBA runner writes:

```text
<out-dir>/
  manifest.env
  abba_schedule.csv
  candidate_gate.md
  baseline_drift_gate.md
  summary.md
  <workload>/<sync>/<leg>/median_summary.csv
  <workload>/<sync>/<leg>/baseline_compare.csv
```

Attach or link at least these files in the issue or PR:

- `summary.md`
- `candidate_gate.md`
- `baseline_drift_gate.md`
- `abba_schedule.csv`
- every `median_summary.csv` and `baseline_compare.csv` for a failed or
  borderline workload
- host telemetry files used to explain CPU, memory, or disk saturation

## Interpretation

Use this decision table:

| Candidate gate | A2 drift gate | Interpretation |
| --- | --- | --- |
| PASS | PASS | Candidate is acceptable for the measured matrix. |
| WARN | PASS | Candidate has a small measurable signal; inspect telemetry and decide if it is expected. |
| FAIL | PASS | Candidate likely regressed the affected workload; investigate before merge. |
| FAIL | FAIL on the same workload | Environment drift is high; rerun on a quieter runner or increase duration and rounds. |
| PASS | FAIL | Candidate did not exceed the budget, but the rig was unstable; avoid using the numbers as proof of improvement. |

When `B1` and `B2` disagree, treat the result as inconclusive even if the gate
passes. Increase duration, rounds, cooldown, or runner isolation before drawing
a conclusion.

## AI execution checklist

When delegating the run to an AI agent or an automation runner, provide these
inputs explicitly:

- repository checkout and candidate branch or commit;
- baseline commit or binary path;
- candidate binary path;
- runner type: local Linux or external cluster;
- endpoint, access key, secret key source, and region;
- deploy hook path or exact command for cluster mode;
- output directory;
- required duration, rounds, cooldown, concurrency, and fail/warn budgets;
- where to upload artifacts after the run.

The AI agent should execute this sequence:

1. Confirm `uname -a`, RustFS commits, binary SHA256 sums, `warp --version`,
   CPU model, memory size, disk layout, and whether the run is local or cluster.
2. Run `scripts/run_hotpath_warp_abba.sh --dry-run` with the final arguments.
3. Run the real ABBA command with `--rounds >= 3`.
4. For samply CPU attribution, use `scripts/run_samply_attach_window.sh` for
   each bounded attach window and reject the cell if the profile is empty or a
   stale `samply` process remains.
5. Preserve the full output directory without editing generated CSV files.
6. Read `summary.md`, `candidate_gate.md`, and `baseline_drift_gate.md`.
7. Summarize only measured facts: candidate deltas, baseline drift, CPU or
   memory saturation, and any failed workloads.
8. Post the summary and artifact location to the tracking issue or PR.

Do not report a performance win or loss when the baseline drift gate failed on
the same workload and no rerun was collected.
