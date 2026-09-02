# Hotpath warp runbook (A/B gate and ABBA evidence)

**Use this when:** you need S3-face (PutObject / GetObject / mixed) performance evidence for a code change: the nightly relative-budget gate, a quick local A/B, or a formal ABBA run whose numbers will be quoted in a PR.
**Source of truth:** `scripts/run_hotpath_warp_ab.sh` (quick A/B rig), `scripts/run_hotpath_warp_abba.sh` (ABBA runner; also what CI executes), `scripts/hotpath_warp_ab_gate.sh` (relative-budget gate), `scripts/run_object_batch_bench_enhanced.sh` (warp driver, medians, `baseline_compare.csv`), `.github/workflows/performance-ab.yml` (CI gate).

This runbook compares two binaries. To sweep one `RUSTFS_*` runtime knob at a time against a fixed binary, use [object-io-tuning-ab-matrix.md](object-io-tuning-ab-matrix.md) instead.

Two entry points share one workload matrix, one gate, and one deploy-hook shape:

| Entry | Script | Legs per cell | Use |
| --- | --- | --- | --- |
| Quick A/B | `scripts/run_hotpath_warp_ab.sh` | baseline → candidate | local smoke, fast triage of a suspected regression |
| ABBA | `scripts/run_hotpath_warp_abba.sh` | A1 baseline → B1 candidate → B2 candidate → A2 baseline | formal evidence; the CI gate |

This rig is the only entry point for S3-face performance coverage. There is deliberately no in-process criterion benchmark for those operations: a criterion harness that stands up an embedded server measures the harness, not the S3 path. Micro-benchmarks stay at function level (EC encode, `xl.meta` parse, `rename_data`). Knob-level sweeps against a fixed binary are a different question; see [object-io-tuning-ab-matrix.md](object-io-tuning-ab-matrix.md).

## Shared prerequisites

- Linux host. A laptop is acceptable for a quick A/B smoke only; formal evidence needs a dedicated runner or a cluster.
- `warp` on `PATH`, or `--warp-bin <path>`.
- Two Linux release binaries, baseline and candidate (build each with `cargo build --release -p rustfs --bins` at its commit; cross-compile with `cargo zigbuild --release --target x86_64-unknown-linux-gnu -p rustfs --bins` for a cluster).
- Disposable disks or data root in local mode; an isolated benchmark bucket and credentials in cluster mode. Never bench against production data.
- Readiness polling (`--health-timeout`, default 180 s in both scripts) must outlast the server's own startup budget, `DEFAULT_STARTUP_READINESS_MAX_WAIT_SECS` in `crates/config/src/constants/health.rs`; a shorter poll misreports a slow cold start as a failure.
- Metric directions: `reqps` (put obj/s) and `throughput` (get MiB/s) are higher-is-better; `latency`/p99 (mixed) is lower-is-better.

## Workload matrix

Every workload runs with `RUSTFS_DRIVE_SYNC_ENABLE=true` and `=false`. Quick A/B: 6 × 2 × 2 = 24 cells; ABBA: 6 × 2 × 4 = 48 cells.

| Workload | mode | size | Why |
| --- | --- | --- | --- |
| `put-4kib` / `get-4kib` | put / get | 4 KiB | small-object fsync-sensitive path |
| `put-4mib` / `get-4mib` | put / get | 4 MiB | bulk obj/s and MiB/s |
| `get-10mib` | get | 10 MiB | large-GET streaming path |
| `mixed-256k` | mixed | 256 KiB | p99 latency |

Sizes are passed to the driver as `--sizes` (one per cell); any size in the driver's `DEFAULT_SIZES` (`scripts/run_object_batch_bench_enhanced.sh`) can be added by editing the `WORKLOADS` array in the chosen script.

## Gate

`scripts/hotpath_warp_ab_gate.sh` compares each metric against baseline: a regression beyond `--fail-pct` (default `FAIL_PCT=10`) fails, beyond `--warn-pct` (default `WARN_PCT=5`) warns. `--allow-regression --exemption-reason "<why>"` records a FAIL as an exempted WARN and exits 0; use it only for a deliberate correctness trade (for example paying write cost to restore power-loss durability).

## Deploy-hook contract (external / cluster mode)

In external mode (`--endpoint <host:port>`) the rig never starts or restarts RustFS. Before each phase or leg it runs `--deploy-hook <cmd>` with the context below in the environment, then waits for `http://<endpoint><health-path>` (default `/health`). A non-zero hook exit aborts the run.

| Variable | Quick A/B | ABBA | Value |
| --- | --- | --- | --- |
| `HOTPATH_AB_PHASE` / `HOTPATH_ABBA_PHASE` | yes | yes | `baseline` or `candidate` |
| `HOTPATH_AB_BINARY` / `HOTPATH_ABBA_BINARY` | yes | yes | selected binary path (A/B: may be empty if the hook builds its own) |
| `HOTPATH_AB_DRIVE_SYNC` / `HOTPATH_ABBA_DRIVE_SYNC` | yes | yes | `true` or `false` for this cell |
| `HOTPATH_ABBA_LEG` | — | yes | `A1`, `B1`, `B2`, `A2` |
| `HOTPATH_ABBA_WORKLOAD`, `HOTPATH_ABBA_MODE`, `HOTPATH_ABBA_SIZE`, `HOTPATH_ABBA_CELL_ID` | — | yes | cell identity |
| `HOTPATH_ABBA_DATASET_NAMESPACE`, `HOTPATH_ABBA_BUCKET` | — | yes | run namespace and per-leg benchmark bucket the hook must provision or reset |
| `HOTPATH_ABBA_DEPLOY_EVIDENCE_FILE` | — | yes | path the hook must write a non-empty evidence file to |

The ABBA runner refuses external mode without a hook unless `--allow-unmanaged-external` is passed, and output from that mode is not formal evidence.

Ansible-shaped hook (replace `/path/to/ansible` and the inventory group; the `config` tag must thread `RUSTFS_DRIVE_SYNC_ENABLE`, or the finer `RUSTFS_DURABILITY_MODE`, into the deployed unit):

```bash
--deploy-hook '
  set -euo pipefail
  cd /path/to/ansible
  cp "${HOTPATH_ABBA_BINARY:?}" roles/rustfs/files/rustfs
  export RUSTFS_DRIVE_SYNC_ENABLE="${HOTPATH_ABBA_DRIVE_SYNC:?}"
  ansible-playbook -f 4 -l bench rustfs-manage.yml --tags stop
  ansible-playbook -f 4 -l bench rustfs-manage.yml --tags config
  ansible-playbook -f 4 -l bench rustfs-manage.yml --tags binary-copy
  ansible-playbook -f 4 -l bench rustfs-manage.yml --tags start
'
```

For the quick A/B rig use the `HOTPATH_AB_*` names.

## Quick A/B (`run_hotpath_warp_ab.sh`)

```bash
# build both binaries (baseline from --baseline-ref, default origin/main) and run a throwaway single-node server on local disks
scripts/run_hotpath_warp_ab.sh --baseline-ref origin/main

# prebuilt binaries
scripts/run_hotpath_warp_ab.sh --skip-build --baseline-bin ./rustfs-main --candidate-bin ./target/release/rustfs

# print the plan only
scripts/run_hotpath_warp_ab.sh --dry-run --skip-build --baseline-bin /tmp/base --candidate-bin /tmp/cand

# external cluster
scripts/run_hotpath_warp_ab.sh --endpoint "$CLUSTER_ENDPOINT" --deploy-hook '<see above>' \
  --baseline-bin /path/to/rustfs-main --candidate-bin ./target/x86_64-unknown-linux-gnu/release/rustfs
```

Outputs under `target/hotpath-ab/<ts>/`: `gate.md` (ends with a **Provenance** section: baseline/candidate SHAs, binary source, runner, warp version, matrix params; extend with `--provenance-note`) and `server-logs/<phase>-sync-<sync>.{log,env}`. On a health-check failure the rig prints the last 50 server log lines; in local mode it fails fast if the server process exits before becoming healthy.

## ABBA evidence run (`run_hotpath_warp_abba.sh`)

`B1` and `B2` are compared with `A1` for the candidate delta; `A2` is compared with `A1` for baseline drift. Required flags: `--baseline-bin`, `--candidate-bin`, `--baseline-revision`, `--candidate-revision`. The script enforces `--rounds >= 3`; prefer `--rounds 5` or more for formal evidence when budget allows.

```bash
# local Linux runner (throwaway data root; a reused run namespace is rejected)
scripts/run_hotpath_warp_abba.sh \
  --baseline-bin /tmp/rustfs-baseline --candidate-bin /tmp/rustfs-candidate \
  --baseline-revision "$(git rev-parse origin/main)" --candidate-revision "$(git rev-parse HEAD)" \
  --address 127.0.0.1:9000 --data-root /var/tmp/rustfs-hotpath-abba --disks 4 \
  --duration 120s --rounds 3 --cooldown 30 --concurrency 16 \
  --out-dir target/hotpath-abba/linux-local

# production-like cluster
scripts/run_hotpath_warp_abba.sh \
  --baseline-bin /srv/rustfs-binaries/rustfs-baseline --candidate-bin /srv/rustfs-binaries/rustfs-candidate \
  --baseline-revision <sha> --candidate-revision <sha> \
  --endpoint rustfs-bench.example.internal:9000 --deploy-hook '<see above>' \
  --duration 180s --rounds 5 --cooldown 45 --concurrency 32 \
  --out-dir target/hotpath-abba/cluster-pr-XXXX
```

Add `--dry-run` to print the schedule without starting servers or warp. Output layout:

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

Attach to the PR or issue: `summary.md`, `candidate_gate.md`, `baseline_drift_gate.md`, `abba_schedule.csv`, every `median_summary.csv` and `baseline_compare.csv` for a failed or borderline workload, and the host telemetry used to explain saturation. Preserve the output directory unmodified.

### Interpretation

| Candidate gate | A2 drift gate | Interpretation |
| --- | --- | --- |
| PASS | PASS | Candidate acceptable for the measured matrix. |
| WARN | PASS | Small measurable signal; inspect telemetry and decide whether it is expected. |
| FAIL | PASS | Candidate likely regressed the workload; investigate before merge. |
| FAIL | FAIL on the same workload | Environment drift is high; rerun on a quieter runner or raise duration and rounds. |
| PASS | FAIL | Rig unstable; do not quote the numbers as proof of improvement. |

Rules that override the table: a candidate result is actionable only when the `A2` drift for the same workload passes or is materially smaller than the `B1`/`B2` delta; never report a win or loss for a workload whose drift gate failed without a rerun. When `B1` and `B2` disagree, the cell is inconclusive even if the gate passes. Report only measured facts: deltas, drift, saturation, failed workloads.

### CPU and memory evidence

Warp output says whether throughput or latency changed; host telemetry says why. Collect it for the whole run and stop the collectors after the script exits.

| Tool | Command | Answers |
| --- | --- | --- |
| `pidstat` | `pidstat -durh 5 > <out-dir>/telemetry/pidstat.txt &` | per-process CPU, memory, disk |
| `mpstat` | `mpstat 5 > <out-dir>/telemetry/mpstat.txt &` | CPU saturation and steal |
| `iostat` | `iostat -xz 5 > <out-dir>/telemetry/iostat.txt &` | device queue depth and latency |
| `perf` | `perf record -F 99 -g -- sleep 180` around one representative cell, then `perf report --stdio` | CPU attribution after the gate shows an effect |

samply against a running RustFS process goes through the bounded helper, one attach window per leg or focused cell:

```bash
scripts/run_samply_attach_window.sh --pid "$RUSTFS_PID" --duration-secs 180 \
  --output <out-dir>/telemetry/samply-A1-get-4mib.json.gz
```

After each window confirm the `.json.gz` profile and its `.syms.json` sidecar are non-empty, no `samply` process is still attached to the PID, and any temporary `perf_event_paranoid` change is restored; reject the cell otherwise.

Instrumented builds (features in `rustfs/Cargo.toml`): `--features hotpath-alloc` for allocation attribution, `--features hotpath-cpu` for CPU hotpath sections. Compare instrumented binaries only with other builds of the same mode; never use them for throughput acceptance, because the instrumentation changes what is measured.

## CI gate (`performance-ab.yml`)

| Aspect | Value |
| --- | --- |
| Triggers | `schedule` (nightly cron `31 6 * * *` UTC against `main`) and `workflow_dispatch` (inputs `duration`, default `12s`; `allow_regression`, boolean). No `pull_request` trigger and no label gating. |
| Jobs | `warp-ab` (runner `sm-standard-2`, `timeout-minutes: 180`); `alert-on-failure` (opens the scheduled-failure issue; scheduled runs only). |
| Baseline commit | scheduled: head of the last successful scheduled run (falls back to the candidate itself when there is none); dispatch: `origin/main`. The baseline must be an ancestor of the candidate. |
| Binary cache | `actions/cache` inside `warp-ab`, key `rustfs-baseline-<baseline_sha>`. Miss: source build (same-commit runs build once and reuse the binary for both phases); a run whose gate passes saves the candidate binary under `rustfs-baseline-<candidate_sha>` for the next night. |
| Command | `scripts/run_hotpath_warp_abba.sh --duration <input> --rounds 3 --cooldown 5 --health-timeout 180 --baseline-revision <sha> --candidate-revision <sha> --baseline-bin ... --candidate-bin ...` |
| Exemption | `allow_regression=true` on dispatch adds `--allow-regression --exemption-reason "workflow dispatch override"`. |
| Artifacts | `hotpath-warp-ab-<run_number>` containing `target/hotpath-abba/` (14-day retention); the step summary renders `candidate_gate.md`, or the server-log tails when the rig failed before the gate. The `Enforce gate` step fails the job on a non-zero rig exit. |

The nightly detects a regression within a day of landing; it does not block a merge. For pre-merge evidence run the ABBA procedure above and attach the outputs to the PR.
