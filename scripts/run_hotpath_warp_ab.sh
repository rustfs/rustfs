#!/usr/bin/env bash
# run_hotpath_warp_ab.sh — Linux warp A/B rig for the hotpath series
# (rustfs/backlog#935 HP-14, item 4).
#
# Runs the same warp workloads against a baseline binary and a candidate
# binary on the same host, across the drive-sync on/off matrix, then applies
# the relative-budget gate (hotpath_warp_ab_gate.sh). This is how the macOS
# profiling conclusions of the HP series get confirmed or corrected on Linux:
# structural wins (call counts, read amplification) should hold; absolute
# numbers are whatever this rig measures.
#
# Load generation and result parsing are delegated to the existing
# scripts/run_object_batch_bench_enhanced.sh (warp driver + median +
# baseline_compare deltas). Server lifecycle follows the single-node local-disk
# pattern of scripts/restart_local_single_node_multidisk_rustfs.sh. warp is
# assumed pre-installed (as everywhere else in scripts/), like in
# run_four_node_cluster_failover_bench.sh.
#
# The rig has been shellcheck- and --dry-run-validated; its first real
# measurement run belongs on a Linux runner with warp installed (see
# .github/workflows/performance-ab.yml).

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
ADDRESS="127.0.0.1:9000"
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

# Workloads: name|warp-mode|size. put-4mib obj/s, get-4mib MiB/s, mixed-256k p99.
WORKLOADS=("put-4mib|put|4MiB" "get-4mib|get|4MiB" "mixed-256k|mixed|256KiB")
# Durability matrix: label|RUSTFS_DRIVE_SYNC_ENABLE value.
DRIVE_SYNC_MATRIX=("sync-on|true" "sync-off|false")

usage() {
  cat <<'USAGE'
Usage: run_hotpath_warp_ab.sh [options]

  --baseline-ref <ref>    Git ref to build the baseline from (default origin/main).
  --baseline-bin <path>   Prebuilt baseline binary (skips building the ref).
  --candidate-bin <path>  Prebuilt candidate binary (default: build the worktree).
  --disks <n>             Local disks for the single node (default 4).
  --concurrency <n>       warp --concurrent (default 8).
  --duration <dur>        warp --duration (default 30s).
  --rounds <n>            Rounds per cell for the median (default 3).
  --fail-pct <n>          Gate fail budget (default 10).
  --warn-pct <n>          Gate warn budget (default 5).
  --allow-regression      Pass the gate despite a FAIL (deliberate tradeoff).
  --exemption-reason <s>  Reason recorded with --allow-regression.
  --out-dir <path>        Output directory (default target/hotpath-ab/<ts>).
  --skip-build            Do not build; requires --baseline-bin/--candidate-bin.
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
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --rounds) ROUNDS="$2"; shift 2 ;;
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

log() { printf '[hotpath-ab] %s\n' "$*" >&2; }

run() {
  # DRY-RUN lines go to stderr so functions that return a value on stdout
  # (e.g. run_cell echoing its compare CSV path) stay uncontaminated.
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

mkdir -p "$OUT_DIR"
log "output dir: $OUT_DIR"

# --- Build binaries ---------------------------------------------------------
build_ref_binary() {
  # Build $1 (a git ref) into a dedicated binary and echo its path.
  local ref="$1" dest="$OUT_DIR/rustfs-baseline"
  if [[ -n "$BASELINE_BIN" ]]; then echo "$BASELINE_BIN"; return; fi
  local wt="$OUT_DIR/baseline-worktree"
  run git worktree add --detach "$wt" "$ref"
  run bash -c "cd '$wt' && cargo build --release --bin rustfs"
  run cp "$wt/target/release/rustfs" "$dest" 2>/dev/null || true
  run git worktree remove --force "$wt" 2>/dev/null || true
  echo "$dest"
}

if [[ "$SKIP_BUILD" == "true" ]]; then
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
log "baseline binary:  $BASELINE_BIN"
log "candidate binary: $CANDIDATE_BIN"

# --- Server lifecycle (single node, local disks) ----------------------------
SERVER_PID=""
stop_server() {
  [[ -n "$SERVER_PID" ]] || return 0
  run kill "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}
cleanup() { stop_server; }
trap cleanup EXIT INT TERM

start_server() {
  # start_server <binary> <drive_sync_value> <node_dir>
  local bin="$1" drive_sync="$2" node_dir="$3"
  local disks=()
  local d
  for ((d = 1; d <= DISKS; d++)); do disks+=("$node_dir/d$d"); done
  run mkdir -p "${disks[@]}"
  if [[ "$DRY_RUN" == "true" ]]; then
    {
      printf 'DRY-RUN: RUSTFS_DRIVE_SYNC_ENABLE=%s %q server' "$drive_sync" "$bin"
      printf ' %q' "${disks[@]}"; printf '\n'
    } >&2
    SERVER_PID="dry-run"
    return 0
  fi
  RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true \
  RUSTFS_ADDRESS="$ADDRESS" \
  RUSTFS_ACCESS_KEY="$ACCESS_KEY" \
  RUSTFS_SECRET_KEY="$SECRET_KEY" \
  RUSTFS_REGION="$REGION" \
  RUSTFS_CONSOLE_ENABLE=false \
  RUSTFS_DRIVE_SYNC_ENABLE="$drive_sync" \
    "$bin" server "${disks[@]}" >"$node_dir/rustfs.log" 2>&1 &
  SERVER_PID=$!
  wait_health
}

wait_health() {
  [[ "$DRY_RUN" == "true" ]] && return 0
  local i
  for ((i = 0; i < 60; i++)); do
    if curl -fsS "http://${ADDRESS}/health" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "error: server did not become healthy" >&2
  return 1
}

# --- One A/B cell: baseline then candidate for a workload x drive-sync -------
run_cell() {
  # run_cell <workload_name> <warp_mode> <size> <sync_label> <drive_sync>
  local wl="$1" mode="$2" size="$3" sync_label="$4" drive_sync="$5"
  local cell="$OUT_DIR/$wl/$sync_label"
  local base_out="$cell/baseline" cand_out="$cell/candidate"
  run mkdir -p "$base_out" "$cand_out"

  local bench_common=(
    --tool warp --warp-bin "$WARP_BIN" --warp-mode "$mode"
    --endpoint "$ADDRESS" --access-key "$ACCESS_KEY" --secret-key "$SECRET_KEY"
    --region "$REGION" --sizes "$size" --concurrency "$CONCURRENCY"
    --duration "$DURATION" --rounds "$ROUNDS"
  )

  log "cell $wl/$sync_label: baseline"
  start_server "$BASELINE_BIN" "$drive_sync" "$DATA_ROOT/$wl-$sync_label-baseline"
  run "$ENHANCED_BENCH" "${bench_common[@]}" --out-dir "$base_out"
  stop_server

  log "cell $wl/$sync_label: candidate (vs baseline)"
  start_server "$CANDIDATE_BIN" "$drive_sync" "$DATA_ROOT/$wl-$sync_label-candidate"
  run "$ENHANCED_BENCH" "${bench_common[@]}" --out-dir "$cand_out" \
    --baseline-csv "$base_out/median_summary.csv"
  stop_server

  # The candidate run wrote baseline_compare.csv; collect it for the gate.
  echo "$cand_out/baseline_compare.csv"
}

# --- Drive the matrix -------------------------------------------------------
declare -a COMPARE_CSVS=()
for wl_spec in "${WORKLOADS[@]}"; do
  IFS='|' read -r wl mode size <<<"$wl_spec"
  for ds_spec in "${DRIVE_SYNC_MATRIX[@]}"; do
    IFS='|' read -r sync_label drive_sync <<<"$ds_spec"
    compare_csv="$(run_cell "$wl" "$mode" "$size" "$sync_label" "$drive_sync")"
    COMPARE_CSVS+=("$compare_csv")
  done
done

# --- Gate -------------------------------------------------------------------
gate_args=(--fail-pct "$FAIL_PCT" --warn-pct "$WARN_PCT" --markdown "$OUT_DIR/gate.md")
for csv in "${COMPARE_CSVS[@]}"; do gate_args+=(--compare-csv "$csv"); done
[[ "$ALLOW_REGRESSION" == "true" ]] && gate_args+=(--allow-regression --exemption-reason "$EXEMPTION_REASON")

if [[ "$DRY_RUN" == "true" ]]; then
  log "dry-run complete; would gate on ${#COMPARE_CSVS[@]} compare CSV(s)"
  printf 'DRY-RUN:'; printf ' %q' "$GATE" "${gate_args[@]}"; printf '\n'
  exit 0
fi

log "applying relative-budget gate"
"$GATE" "${gate_args[@]}"
gate_status=$?
log "gate result written to $OUT_DIR/gate.md (exit $gate_status)"
exit "$gate_status"
