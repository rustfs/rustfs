#!/usr/bin/env bash
# run_hotpath_warp_ab.sh — Linux warp A/B rig for the hotpath series
# (rustfs/backlog#935 HP-14, item 4).
#
# Runs the same warp workloads against a baseline binary and a candidate
# binary, across the drive-sync on/off matrix, then applies the relative-budget
# gate (hotpath_warp_ab_gate.sh). This is how the macOS profiling conclusions
# of the HP series get confirmed or corrected on Linux: structural wins (call
# counts, read amplification) should hold; absolute numbers are whatever this
# rig measures.
#
# Two deployment modes:
#   * local (default): build both binaries and run a throwaway single-node
#     server on local disks — good for a quick or CI smoke A/B.
#   * external (--endpoint <host:port>): warp targets an already-running
#     cluster; a --deploy-hook command swaps in the right binary / durability
#     config between phases. This maps onto the team's ansible harness
#     (cargo zigbuild -> ansible rustfs-install / rustfs-manage --tags
#     stop,config,binary-copy,start -> warp). See
#     docs/operations/hotpath-warp-ab-runbook.md.
#
# Load generation and result parsing are delegated to the existing
# scripts/run_object_batch_bench_enhanced.sh (warp driver + median +
# baseline_compare deltas). warp is assumed pre-installed, as elsewhere in
# scripts/.
#
# The rig is shellcheck- and --dry-run-validated; its first real measurement
# run belongs on a Linux runner / cluster with warp installed.

set -euo pipefail

PROJECT_ROOT="$(git rev-parse --show-toplevel)"
ENHANCED_BENCH="${PROJECT_ROOT}/scripts/run_object_batch_bench_enhanced.sh"
GATE="${PROJECT_ROOT}/scripts/hotpath_warp_ab_gate.sh"

# --- Configuration (overridable via flags) ----------------------------------
BASELINE_REF="origin/main"
BASELINE_BIN=""
CANDIDATE_BIN=""
DISKS=4
CONCURRENCY=8
DURATION="30s"
ROUNDS=3
COOLDOWN_SECS=""         # empty -> load driver default; CI passes a short value
LOCAL_ADDRESS="127.0.0.1:9000"
ACCESS_KEY="rustfsadmin"
SECRET_KEY="rustfsadmin"
REGION="us-east-1"
WARP_BIN="warp"
FAIL_PCT=10
WARN_PCT=5
ALLOW_REGRESSION="false"
EXEMPTION_REASON="deliberate correctness tradeoff"
DATA_ROOT="/tmp/rustfs-hotpath-ab"
OUT_DIR="${PROJECT_ROOT}/target/hotpath-ab/$(date -u +%Y%m%dT%H%M%SZ 2>/dev/null || echo run)"
DRY_RUN="false"
SKIP_BUILD="false"
EXTERNAL_ENDPOINT=""     # set -> external mode (warp targets this cluster)
DEPLOY_HOOK=""           # external mode: command to deploy a phase's binary
HEALTH_PATH="/health"
# Readiness poll budget. The server binds its public listener only after startup
# converges (erasure-format load + IAM); its own budget is
# RUSTFS_STARTUP_READINESS_MAX_WAIT_SECS (default 120s). Polling must outlast
# that, otherwise a slow cold start on a shared CI runner looks like a failure —
# this is exactly what killed the first two nightly runs (60s poll < 120s
# startup budget). Default 180s; override with --health-timeout.
HEALTH_TIMEOUT_SECS=180

# Workloads: name|warp-mode|size.
#   put-4kib / get-4kib — the #4221 fsync regression size (~-10% @4KiB), invisible
#     until now; put-4mib / get-4mib — bulk obj/s & MiB/s; get-10mib — the historical
#     large-GET EOF size; mixed-256k — p99 latency.
# Tradeoff: 6 workloads × 2 phases × 2 drive-sync = 24 cells. Until perf-3 caches
# the baseline binary (the ~65min double build dominates the 90min job), CI keeps
# the matrix under budget with short warp params (--duration/--rounds/--cooldown
# passed by the workflow), NOT by dropping cells. A 1KiB cell (optional per
# perf-1) is deferred until that caching frees wall-clock; re-enable by adding
# e.g. "put-1kib|put|1KiB" here.
WORKLOADS=(
  "put-4kib|put|4KiB"
  "put-4mib|put|4MiB"
  "get-4kib|get|4KiB"
  "get-4mib|get|4MiB"
  "get-10mib|get|10MiB"
  "mixed-256k|mixed|256KiB"
)
# Durability matrix: label|RUSTFS_DRIVE_SYNC_ENABLE value.
DRIVE_SYNC_MATRIX=("sync-on|true" "sync-off|false")

usage() {
  cat <<'USAGE'
Usage: run_hotpath_warp_ab.sh [options]

Local mode (default): build + run a throwaway single-node server.
  --baseline-ref <ref>    Git ref to build the baseline from (default origin/main).
  --baseline-bin <path>   Prebuilt baseline binary (skips building the ref).
  --candidate-bin <path>  Prebuilt candidate binary (default: build the worktree).
  --disks <n>             Local disks for the single node (default 4).
  --skip-build            Do not build; requires --baseline-bin/--candidate-bin.

External mode: warp targets an already-running cluster (e.g. ansible-deployed).
  --endpoint <host:port>  Cluster S3 endpoint; enables external mode.
  --deploy-hook <cmd>     Command run before each phase to deploy that phase's
                          binary and durability config. It receives context via
                          env: HOTPATH_AB_PHASE (baseline|candidate),
                          HOTPATH_AB_BINARY (path or empty),
                          HOTPATH_AB_DRIVE_SYNC (true|false). Non-zero aborts.
  --health-path <path>    Readiness path polled after deploy (default /health).

Common:
  --concurrency <n>       warp --concurrent (default 8).
  --duration <dur>        warp --duration (default 30s).
  --rounds <n>            Rounds per cell for the median (default 3).
  --cooldown <n>          Seconds between rounds/sizes, forwarded to the load
                          driver (default: driver default, 20s).
  --health-timeout <n>    Seconds to poll readiness after bring-up (default 180).
  --fail-pct <n>          Gate fail budget (default 10).
  --warn-pct <n>          Gate warn budget (default 5).
  --allow-regression      Pass the gate despite a FAIL (deliberate tradeoff).
  --exemption-reason <s>  Reason recorded with --allow-regression.
  --out-dir <path>        Output directory (default target/hotpath-ab/<ts>).
  --dry-run               Print the plan and commands without running them.
  -h, --help              Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --baseline-ref) BASELINE_REF="$2"; shift 2 ;;
    --baseline-bin) BASELINE_BIN="$2"; shift 2 ;;
    --candidate-bin) CANDIDATE_BIN="$2"; shift 2 ;;
    --disks) DISKS="$2"; shift 2 ;;
    --endpoint) EXTERNAL_ENDPOINT="$2"; shift 2 ;;
    --deploy-hook) DEPLOY_HOOK="$2"; shift 2 ;;
    --health-path) HEALTH_PATH="$2"; shift 2 ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --rounds) ROUNDS="$2"; shift 2 ;;
    --cooldown) COOLDOWN_SECS="$2"; shift 2 ;;
    --health-timeout) HEALTH_TIMEOUT_SECS="$2"; shift 2 ;;
    --fail-pct) FAIL_PCT="$2"; shift 2 ;;
    --warn-pct) WARN_PCT="$2"; shift 2 ;;
    --allow-regression) ALLOW_REGRESSION="true"; shift ;;
    --exemption-reason) EXEMPTION_REASON="$2"; shift 2 ;;
    --out-dir) OUT_DIR="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD="true"; shift ;;
    --dry-run) DRY_RUN="true"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

EXTERNAL="false"
[[ -n "$EXTERNAL_ENDPOINT" ]] && EXTERNAL="true"
ADDRESS="$EXTERNAL_ENDPOINT"
[[ "$EXTERNAL" == "true" ]] || ADDRESS="$LOCAL_ADDRESS"

log() { printf '[hotpath-ab] %s\n' "$*" >&2; }

run() {
  # DRY-RUN lines go to stderr so functions that return a value on stdout stay
  # uncontaminated.
  if [[ "$DRY_RUN" == "true" ]]; then
    { printf 'DRY-RUN:'; printf ' %q' "$@"; printf '\n'; } >&2
    return 0
  fi
  "$@"
}

[[ -x "$ENHANCED_BENCH" ]] || { echo "missing load driver: $ENHANCED_BENCH" >&2; exit 2; }
[[ -x "$GATE" ]] || { echo "missing gate: $GATE" >&2; exit 2; }
if [[ "$DRY_RUN" != "true" ]] && ! command -v "$WARP_BIN" >/dev/null 2>&1; then
  echo "error: warp not found on PATH (install warp or use --dry-run)" >&2
  exit 2
fi
if [[ "$EXTERNAL" == "true" && -z "$DEPLOY_HOOK" ]]; then
  log "warning: external mode without --deploy-hook — baseline and candidate must be swapped out of band"
fi

mkdir -p "$OUT_DIR"
log "mode: $([[ "$EXTERNAL" == "true" ]] && echo "external ($ADDRESS)" || echo "local ($ADDRESS)")"
log "output dir: $OUT_DIR"

# --- Build binaries (local mode only) ---------------------------------------
build_ref_binary() {
  local ref="$1" dest="$OUT_DIR/rustfs-baseline"
  if [[ -n "$BASELINE_BIN" ]]; then echo "$BASELINE_BIN"; return; fi
  local wt="$OUT_DIR/baseline-worktree"
  run git worktree add --detach "$wt" "$ref"
  run bash -c "cd '$wt' && cargo build --release --bin rustfs"
  run cp "$wt/target/release/rustfs" "$dest" 2>/dev/null || true
  run git worktree remove --force "$wt" 2>/dev/null || true
  echo "$dest"
}

if [[ "$EXTERNAL" == "true" ]]; then
  # Binaries are optional context handed to the deploy hook.
  log "baseline binary:  ${BASELINE_BIN:-<managed by deploy hook>}"
  log "candidate binary: ${CANDIDATE_BIN:-<managed by deploy hook>}"
elif [[ "$SKIP_BUILD" == "true" ]]; then
  [[ -n "$BASELINE_BIN" && -n "$CANDIDATE_BIN" ]] || {
    echo "error: --skip-build requires --baseline-bin and --candidate-bin" >&2; exit 2; }
else
  [[ -n "$CANDIDATE_BIN" ]] || {
    log "building candidate (current worktree)"
    run cargo build --release --bin rustfs
    CANDIDATE_BIN="${PROJECT_ROOT}/target/release/rustfs"
  }
  [[ -n "$BASELINE_BIN" ]] || {
    log "building baseline ($BASELINE_REF)"
    BASELINE_BIN="$(build_ref_binary "$BASELINE_REF")"
  }
fi

# --- Deployment: bring a phase up, tear it down -----------------------------
# Server logs live under OUT_DIR (not $DATA_ROOT) so they ride along in the CI
# artifact and the run is diagnosable after the fact — the missing piece that
# left the first nightly failures unexplained.
SERVER_LOG_DIR="$OUT_DIR/server-logs"
run mkdir -p "$SERVER_LOG_DIR"
SERVER_PID=""
SERVER_LOG=""
tear_down() {
  # Local mode owns the server process; external mode leaves lifecycle to the
  # deploy hook / cluster orchestrator.
  [[ "$EXTERNAL" == "true" ]] && return 0
  [[ -n "$SERVER_PID" ]] || return 0
  run kill "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}
trap tear_down EXIT INT TERM

# Dump the tail of the current server log to stderr (job log) so a startup
# failure is visible without downloading the artifact.
dump_server_log() {
  [[ -n "$SERVER_LOG" && -f "$SERVER_LOG" ]] || return 0
  echo "----- last 50 lines of $SERVER_LOG -----" >&2
  tail -n 50 "$SERVER_LOG" >&2 || true
  echo "----------------------------------------" >&2
}

wait_health() {
  [[ "$DRY_RUN" == "true" ]] && return 0
  local i
  for ((i = 0; i < HEALTH_TIMEOUT_SECS; i++)); do
    # Local mode owns the process: if it already exited (port bind failure,
    # panic, disk-check abort), stop polling and surface the log immediately
    # instead of waiting out the full budget.
    if [[ "$EXTERNAL" != "true" && -n "$SERVER_PID" ]] && ! kill -0 "$SERVER_PID" 2>/dev/null; then
      echo "error: rustfs server (pid $SERVER_PID) exited before becoming healthy after ${i}s" >&2
      dump_server_log
      return 1
    fi
    if curl -fsS "http://${ADDRESS}${HEALTH_PATH}" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "error: endpoint http://${ADDRESS}${HEALTH_PATH} did not become healthy within ${HEALTH_TIMEOUT_SECS}s" >&2
  dump_server_log
  return 1
}

bring_up() {
  # bring_up <phase> <binary> <drive_sync>
  local phase="$1" bin="$2" drive_sync="$3"
  if [[ "$EXTERNAL" == "true" ]]; then
    if [[ -n "$DEPLOY_HOOK" ]]; then
      log "deploy hook: phase=$phase drive_sync=$drive_sync"
      HOTPATH_AB_PHASE="$phase" HOTPATH_AB_BINARY="$bin" HOTPATH_AB_DRIVE_SYNC="$drive_sync" \
        run bash -c "$DEPLOY_HOOK"
    fi
    wait_health
    return 0
  fi

  # Local: throwaway single-node server on fresh disks.
  local node_dir="$DATA_ROOT/$phase-sync-$drive_sync"
  local disks=() d
  for ((d = 1; d <= DISKS; d++)); do disks+=("$node_dir/d$d"); done
  run mkdir -p "${disks[@]}"
  SERVER_LOG="$SERVER_LOG_DIR/$phase-sync-$drive_sync.log"
  if [[ "$DRY_RUN" == "true" ]]; then
    { printf 'DRY-RUN: RUSTFS_DRIVE_SYNC_ENABLE=%s %q server' "$drive_sync" "$bin"
      printf ' %q' "${disks[@]}"; printf '\n'; } >&2
    SERVER_PID="dry-run"
    return 0
  fi
  # Record the startup environment next to the log so a failed run is
  # self-explanatory in the artifact.
  cat >"$SERVER_LOG_DIR/$phase-sync-$drive_sync.env" <<EOF
phase=$phase
drive_sync=$drive_sync
binary=$bin
address=$ADDRESS
disks=${disks[*]}
health_url=http://${ADDRESS}${HEALTH_PATH}
health_timeout_secs=$HEALTH_TIMEOUT_SECS
uname=$(uname -a 2>/dev/null || echo unknown)
warp_version=$("$WARP_BIN" --version 2>/dev/null | head -n1 || echo unknown)
EOF
  RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true \
  RUSTFS_ADDRESS="$ADDRESS" \
  RUSTFS_ACCESS_KEY="$ACCESS_KEY" \
  RUSTFS_SECRET_KEY="$SECRET_KEY" \
  RUSTFS_REGION="$REGION" \
  RUSTFS_CONSOLE_ENABLE=false \
  RUSTFS_DRIVE_SYNC_ENABLE="$drive_sync" \
    "$bin" server "${disks[@]}" >"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  wait_health
}

# --- Measure one workload against the current deployment --------------------
measure() {
  # measure <phase> <workload> <mode> <size> <sync_label> [baseline_csv]
  local phase="$1" wl="$2" mode="$3" size="$4" sync_label="$5" baseline_csv="${6:-}"
  local cell="$OUT_DIR/$wl/$sync_label/$phase"
  run mkdir -p "$cell"
  local args=(
    --tool warp --warp-bin "$WARP_BIN" --warp-mode "$mode"
    --endpoint "$ADDRESS" --access-key "$ACCESS_KEY" --secret-key "$SECRET_KEY"
    --region "$REGION" --sizes "$size" --concurrency "$CONCURRENCY"
    --duration "$DURATION" --rounds "$ROUNDS" --out-dir "$cell"
  )
  [[ -n "$COOLDOWN_SECS" ]] && args+=(--cooldown-secs "$COOLDOWN_SECS")
  [[ -n "$baseline_csv" ]] && args+=(--baseline-csv "$baseline_csv")
  run "$ENHANCED_BENCH" "${args[@]}"
  echo "$cell"
}

# --- Drive the matrix: per drive-sync, baseline phase then candidate phase ---
declare -a COMPARE_CSVS=()
for ds_spec in "${DRIVE_SYNC_MATRIX[@]}"; do
  IFS='|' read -r sync_label drive_sync <<<"$ds_spec"

  log "=== drive-sync $sync_label: baseline phase ==="
  bring_up "baseline" "$BASELINE_BIN" "$drive_sync"
  for wl_spec in "${WORKLOADS[@]}"; do
    IFS='|' read -r wl mode size <<<"$wl_spec"
    measure "baseline" "$wl" "$mode" "$size" "$sync_label" >/dev/null
  done
  tear_down

  log "=== drive-sync $sync_label: candidate phase (vs baseline) ==="
  bring_up "candidate" "$CANDIDATE_BIN" "$drive_sync"
  for wl_spec in "${WORKLOADS[@]}"; do
    IFS='|' read -r wl mode size <<<"$wl_spec"
    # measure() writes each phase under $OUT_DIR/<wl>/<sync>/<phase>, so the
    # baseline median for this cell is at a deterministic path.
    baseline_csv="$OUT_DIR/$wl/$sync_label/baseline/median_summary.csv"
    cand_cell="$(measure "candidate" "$wl" "$mode" "$size" "$sync_label" "$baseline_csv")"
    COMPARE_CSVS+=("$cand_cell/baseline_compare.csv")
  done
  tear_down
done

# --- Gate -------------------------------------------------------------------
gate_args=(--fail-pct "$FAIL_PCT" --warn-pct "$WARN_PCT" --markdown "$OUT_DIR/gate.md")
for csv in "${COMPARE_CSVS[@]}"; do gate_args+=(--compare-csv "$csv"); done
[[ "$ALLOW_REGRESSION" == "true" ]] && gate_args+=(--allow-regression --exemption-reason "$EXEMPTION_REASON")

if [[ "$DRY_RUN" == "true" ]]; then
  log "dry-run complete; would gate on ${#COMPARE_CSVS[@]} compare CSV(s)"
  { printf 'DRY-RUN:'; printf ' %q' "$GATE" "${gate_args[@]}"; printf '\n'; } >&2
  exit 0
fi

log "applying relative-budget gate"
"$GATE" "${gate_args[@]}"
gate_status=$?
log "gate result written to $OUT_DIR/gate.md (exit $gate_status)"
exit "$gate_status"
