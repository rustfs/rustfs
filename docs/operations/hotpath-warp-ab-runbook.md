# Hotpath warp A/B runbook

Relative-budget A/B gate for the hotpath series (rustfs/backlog#935 HP-14). It
runs the same warp workloads against a **baseline** binary and a **candidate**
binary, across the drive-sync on/off matrix, then applies a relative budget:
a metric regressing past the fail budget fails the gate, past the warn budget
warns. This is how the macOS profiling conclusions of the HP series get
confirmed or corrected on Linux — structural wins (call counts, read
amplification) should hold; absolute numbers are whatever the rig measures.

Pieces:

- `scripts/run_hotpath_warp_ab.sh` — orchestrator (baseline vs candidate,
  workload × drive-sync matrix).
- `scripts/hotpath_warp_ab_gate.sh` — the budget gate over the
  `baseline_compare.csv` deltas the load driver emits.
- `scripts/run_object_batch_bench_enhanced.sh` — the warp driver + median +
  `baseline_compare.csv` (reused, not reimplemented).
- `.github/workflows/performance-ab.yml` — nightly on `main` (post-merge
  detection) plus opt-in pre-merge via the `perf-ab` label.

Metric directions: `reqps` (put obj/s) and `throughput` (get MiB/s) are
higher-is-better; `latency` / p99 (mixed) is lower-is-better. warp is assumed
pre-installed, as elsewhere in `scripts/`.

## Workload matrix

Six workloads × the drive-sync on/off matrix × baseline/candidate = 24 cells:

| Workload | mode | size | why |
| --- | --- | --- | --- |
| `put-4kib` / `get-4kib` | put / get | 4KiB | the #4221 fsync regression size (~-10% @4KiB) — previously invisible |
| `put-4mib` / `get-4mib` | put / get | 4MiB | bulk obj/s and MiB/s |
| `get-10mib` | get | 10MiB | the historical large-GET EOF size |
| `mixed-256k` | mixed | 256KiB | p99 latency |

Sizes are passed to the load driver via `--sizes` (one size per cell); the
driver's `DEFAULT_SIZES` covers 1KiB..10MiB, so any of those can be added by
editing `WORKLOADS` in `scripts/run_hotpath_warp_ab.sh`. A 1KiB cell is left
out for now to keep the nightly matrix under the 90-minute job budget (the
baseline+candidate double build dominates until perf-3 caches it); re-enable it
once that lands.

CI runs a **short** warp matrix (`--duration`/`--rounds`/`--cooldown` tuned in
`.github/workflows/performance-ab.yml`) so all 24 cells fit the budget without
dropping cells. These params are deliberately noisy-but-fast for the Phase-0
"keep the pipeline alive" goal; perf-6 recalibrates them.

## Local mode (quick / CI smoke)

Builds both binaries and runs a throwaway single-node server on local disks.

```bash
scripts/run_hotpath_warp_ab.sh --baseline-ref origin/main
# or with prebuilt binaries:
scripts/run_hotpath_warp_ab.sh --skip-build \
  --baseline-bin ./rustfs-main --candidate-bin ./target/release/rustfs
```

Preview the full plan without running anything:

```bash
scripts/run_hotpath_warp_ab.sh --dry-run --skip-build \
  --baseline-bin /tmp/base --candidate-bin /tmp/cand
```

## External mode (real cluster, ansible-deployed)

For the production-representative run, warp targets an already-running cluster
and a `--deploy-hook` swaps in each phase's binary and durability config
between the baseline and candidate phases. The hook receives context via the
environment:

- `HOTPATH_AB_PHASE` — `baseline` or `candidate`
- `HOTPATH_AB_BINARY` — binary path (or empty; the hook may build its own)
- `HOTPATH_AB_DRIVE_SYNC` — `true` or `false` for this matrix cell

This maps directly onto the team's ansible harness. Build the candidate with
the cross toolchain, stage both binaries, then let the hook drive
`rustfs-manage.yml`:

```bash
# 1. Build the candidate (cross-compile for the cluster target).
cargo zigbuild --release --target x86_64-unknown-linux-gnu -p rustfs --bins

# 2. Run the A/B against the cluster; the hook deploys the phase's binary and
#    applies the drive-sync config, then restarts, before each phase.
scripts/run_hotpath_warp_ab.sh \
  --endpoint "$CLUSTER_ENDPOINT" \
  --deploy-hook '
    set -euo pipefail
    cd /home/xiaomage/xiaomage/ansible
    # Select the phase binary and the drive-sync value for this cell.
    cp "${HOTPATH_AB_BINARY:?}" ./roles/rustfs/files/rustfs
    export RUSTFS_DRIVE_SYNC_ENABLE="$HOTPATH_AB_DRIVE_SYNC"
    ansible-playbook -f 4 -l testing rustfs-manage.yml --tags stop
    ansible-playbook -f 4 -l testing rustfs-manage.yml --tags config
    ansible-playbook -f 4 -l testing rustfs-manage.yml --tags binary-copy
    ansible-playbook -f 4 -l testing rustfs-manage.yml --tags start
  ' \
  --baseline-bin /path/to/rustfs-main \
  --candidate-bin ./target/x86_64-unknown-linux-gnu/release/rustfs
```

The `config` tag is responsible for threading `RUSTFS_DRIVE_SYNC_ENABLE` (or
the finer `RUSTFS_DURABILITY_MODE`) into the deployed unit — the hook exports
it so the config template can pick it up. The rig itself never restarts the
cluster; lifecycle stays with ansible.

## Budget and exemptions

Default budget: a metric regressing more than **10%** vs baseline fails,
more than **5%** warns. Tune with `--fail-pct` / `--warn-pct`.

Some regressions are the correct trade — #4221 deliberately paid a large write
cost to restore power-loss durability. For those, run with
`--allow-regression` (or add the `perf-deliberate-tradeoff` label in CI): the
FAIL is recorded and rendered as an exempted WARN, and the gate exits 0.

## Diagnosing a failed run

Each phase's server log and its startup environment are written under the run's
output dir (`target/hotpath-ab/<ts>/server-logs/<phase>-sync-<sync>.{log,env}`)
and uploaded in the `hotpath-warp-ab-<run>` artifact, so a failure is
diagnosable after the fact. On a health-check failure the rig also dumps the
last 50 log lines into the job log and the CI job writes the failing phase (or
the gate table) into the GitHub step summary.

Readiness polling waits up to `--health-timeout` seconds (default **180**),
which must outlast the server's own startup-readiness budget
(`RUSTFS_STARTUP_READINESS_MAX_WAIT_SECS`, default 120s) — a shorter poll on a
slow shared runner mis-reports a slow cold start as a failure. In local mode the
rig also fails fast if the server process exits before becoming healthy instead
of polling out the full budget.

## Scope note

The gate logic is unit-validated across pass/warn/fail/exempt outcomes; the
orchestrator and workflow are shellcheck- and `--dry-run`-validated. The first
real warp measurement belongs on a Linux runner or the ansible cluster — there
is no warp/multi-disk rig in the repo's local checkout.
