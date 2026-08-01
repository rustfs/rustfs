#!/usr/bin/env bash
# Formal Linux / production-cluster ABBA runner for the hotpath warp matrix.
#
# This script is intentionally a thin orchestrator around the existing
# run_object_batch_bench_enhanced.sh load driver and hotpath_warp_ab_gate.sh
# relative-budget gate. It runs each durability/workload cell as:
#
#   A1 baseline -> B1 candidate -> B2 candidate -> A2 baseline
#
# Candidate legs are compared against A1. The final A2 leg is also compared
# against A1 to quantify baseline drift separately from candidate deltas.

set -euo pipefail

PROJECT_ROOT="$(git rev-parse --show-toplevel)"
ENHANCED_BENCH="${PROJECT_ROOT}/scripts/run_object_batch_bench_enhanced.sh"
GATE="${PROJECT_ROOT}/scripts/hotpath_warp_ab_gate.sh"

BASELINE_BIN=""
CANDIDATE_BIN=""
ENDPOINT=""
DEPLOY_HOOK=""
HEALTH_PATH="/health"
ADDRESS="127.0.0.1:9000"
DATA_ROOT="/tmp/rustfs-hotpath-abba"
DISKS=4
ACCESS_KEY="rustfsadmin"
SECRET_KEY="rustfsadmin"
REGION="us-east-1"
WARP_BIN="warp"
CONCURRENCY=8
DURATION="60s"
ROUNDS=3
COOLDOWN_SECS=20
HEALTH_TIMEOUT_SECS=180
FAIL_PCT=10
WARN_PCT=5
ALLOW_REGRESSION=false
EXEMPTION_REASON="deliberate correctness tradeoff"
OUT_DIR="${PROJECT_ROOT}/target/hotpath-abba/$(date -u +%Y%m%dT%H%M%SZ 2>/dev/null || echo run)"
DRY_RUN=false

WORKLOADS=(
  "put-4kib|put|4KiB"
  "put-4mib|put|4MiB"
  "get-4kib|get|4KiB"
  "get-4mib|get|4MiB"
  "get-10mib|get|10MiB"
  "mixed-256k|mixed|256KiB"
)
DRIVE_SYNC_MATRIX=("sync-on|true" "sync-off|false")

usage() {
  cat <<'USAGE'
Usage: scripts/run_hotpath_warp_abba.sh --baseline-bin <path> --candidate-bin <path> [options]

Formal ABBA mode for Linux runners or production-like clusters. The schedule is
A1 baseline -> B1 candidate -> B2 candidate -> A2 baseline for every workload
and drive-sync cell.

Required:
  --baseline-bin <path>     Baseline RustFS binary.
  --candidate-bin <path>    Candidate RustFS binary.

Local Linux runner mode:
  --address <host:port>     Local RustFS address (default 127.0.0.1:9000).
  --disks <n>               Throwaway local disks per node (default 4).
  --data-root <path>        Local disk root (default /tmp/rustfs-hotpath-abba).

Production / cluster mode:
  --endpoint <host:port>    Existing cluster endpoint. Enables external mode.
  --deploy-hook <cmd>       Command run before each ABBA leg. It receives:
                              HOTPATH_ABBA_LEG=A1|B1|B2|A2
                              HOTPATH_ABBA_PHASE=baseline|candidate
                              HOTPATH_ABBA_BINARY=<baseline/candidate binary>
                              HOTPATH_ABBA_DRIVE_SYNC=true|false
  --health-path <path>      Readiness path (default /health).

Benchmark:
  --duration <dur>          warp duration per cell (default 60s).
  --rounds <n>              rounds per cell; must be >= 3 (default 3).
  --cooldown <n>            cooldown seconds between rounds/sizes (default 20).
  --concurrency <n>         warp concurrency (default 8).
  --warp-bin <path>         warp binary (default warp).

Credentials:
  --access-key <value>      S3 access key (default rustfsadmin).
  --secret-key <value>      S3 secret key (default rustfsadmin).
  --region <value>          S3 region (default us-east-1).

Gate:
  --fail-pct <n>            Regression budget that fails gate (default 10).
  --warn-pct <n>            Regression budget that warns (default 5).
  --allow-regression        Downgrade candidate gate FAIL to WARN.
  --exemption-reason <s>    Reason recorded when allow-regression is used.

Output:
  --out-dir <path>          Output dir (default target/hotpath-abba/<ts>).
  --dry-run                 Print commands without starting servers or warp.
  -h, --help

Outputs:
  <out-dir>/abba_schedule.csv
  <out-dir>/candidate_gate.md
  <out-dir>/baseline_drift_gate.md
  <out-dir>/summary.md
  <out-dir>/<workload>/<sync>/<leg>/{median_summary.csv,baseline_compare.csv}
USAGE
}

die() {
  echo "error: $*" >&2
  exit 2
}

log() {
  printf '[hotpath-abba] %s\n' "$*" >&2
}

run() {
  if [[ "$DRY_RUN" == "true" ]]; then
    { printf 'DRY-RUN:'; printf ' %q' "$@"; printf '\n'; } >&2
    return 0
  fi
  "$@"
}

validate_positive_int() {
  local value="$1" name="$2"
  [[ "$value" =~ ^[0-9]+$ && "$value" -gt 0 ]] || die "$name must be a positive integer"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --baseline-bin) BASELINE_BIN="$2"; shift 2 ;;
    --candidate-bin) CANDIDATE_BIN="$2"; shift 2 ;;
    --endpoint) ENDPOINT="$2"; shift 2 ;;
    --deploy-hook) DEPLOY_HOOK="$2"; shift 2 ;;
    --health-path) HEALTH_PATH="$2"; shift 2 ;;
    --address) ADDRESS="$2"; shift 2 ;;
    --data-root) DATA_ROOT="$2"; shift 2 ;;
    --disks) DISKS="$2"; shift 2 ;;
    --access-key) ACCESS_KEY="$2"; shift 2 ;;
    --secret-key) SECRET_KEY="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --warp-bin) WARP_BIN="$2"; shift 2 ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --rounds) ROUNDS="$2"; shift 2 ;;
    --cooldown) COOLDOWN_SECS="$2"; shift 2 ;;
    --health-timeout) HEALTH_TIMEOUT_SECS="$2"; shift 2 ;;
    --fail-pct) FAIL_PCT="$2"; shift 2 ;;
    --warn-pct) WARN_PCT="$2"; shift 2 ;;
    --allow-regression) ALLOW_REGRESSION=true; shift ;;
    --exemption-reason) EXEMPTION_REASON="$2"; shift 2 ;;
    --out-dir) OUT_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

validate_positive_int "$DISKS" "--disks"
validate_positive_int "$CONCURRENCY" "--concurrency"
validate_positive_int "$ROUNDS" "--rounds"
validate_positive_int "$COOLDOWN_SECS" "--cooldown"
validate_positive_int "$HEALTH_TIMEOUT_SECS" "--health-timeout"
validate_positive_int "$FAIL_PCT" "--fail-pct"
validate_positive_int "$WARN_PCT" "--warn-pct"
[[ "$ROUNDS" -ge 3 ]] || die "--rounds must be >= 3 for formal ABBA evidence"
[[ -n "$BASELINE_BIN" ]] || die "--baseline-bin is required"
[[ -n "$CANDIDATE_BIN" ]] || die "--candidate-bin is required"
[[ "$DRY_RUN" == "true" || -x "$BASELINE_BIN" ]] || die "baseline binary is not executable: $BASELINE_BIN"
[[ "$DRY_RUN" == "true" || -x "$CANDIDATE_BIN" ]] || die "candidate binary is not executable: $CANDIDATE_BIN"
[[ -x "$ENHANCED_BENCH" ]] || die "missing load driver: $ENHANCED_BENCH"
[[ -x "$GATE" ]] || die "missing gate: $GATE"
if [[ "$DRY_RUN" != "true" ]] && ! command -v "$WARP_BIN" >/dev/null 2>&1; then
  die "warp not found on PATH; install warp or pass --warp-bin"
fi

EXTERNAL=false
if [[ -n "$ENDPOINT" ]]; then
  EXTERNAL=true
  ADDRESS="$ENDPOINT"
  [[ -n "$DEPLOY_HOOK" ]] || log "warning: external mode without --deploy-hook; binaries must be swapped out of band"
fi

mkdir -p "$OUT_DIR"
SERVER_LOG_DIR="$OUT_DIR/server-logs"
run mkdir -p "$SERVER_LOG_DIR"

SERVER_PID=""
SERVER_LOG=""
tear_down() {
  [[ "$EXTERNAL" == "true" ]] && return 0
  [[ -n "$SERVER_PID" ]] || return 0
  run kill "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}
trap tear_down EXIT INT TERM

dump_server_log() {
  [[ -n "$SERVER_LOG" && -f "$SERVER_LOG" ]] || return 0
  echo "----- last 80 lines of $SERVER_LOG -----" >&2
  tail -n 80 "$SERVER_LOG" >&2 || true
  echo "----------------------------------------" >&2
}

wait_health() {
  [[ "$DRY_RUN" == "true" ]] && return 0
  local i
  for ((i = 0; i < HEALTH_TIMEOUT_SECS; i++)); do
    if [[ "$EXTERNAL" != "true" && -n "$SERVER_PID" ]] && ! kill -0 "$SERVER_PID" 2>/dev/null; then
      echo "error: rustfs server (pid $SERVER_PID) exited before becoming healthy after ${i}s" >&2
      dump_server_log
      return 1
    fi
    if curl -fsS "http://${ADDRESS}${HEALTH_PATH}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "error: endpoint http://${ADDRESS}${HEALTH_PATH} did not become healthy within ${HEALTH_TIMEOUT_SECS}s" >&2
  dump_server_log
  return 1
}

binary_for_leg() {
  case "$1" in
    A1|A2) echo "$BASELINE_BIN" ;;
    B1|B2) echo "$CANDIDATE_BIN" ;;
    *) die "unknown ABBA leg: $1" ;;
  esac
}

phase_for_leg() {
  case "$1" in
    A1|A2) echo "baseline" ;;
    B1|B2) echo "candidate" ;;
    *) die "unknown ABBA leg: $1" ;;
  esac
}

bring_up() {
  local leg="$1" drive_sync="$2"
  local phase bin
  phase="$(phase_for_leg "$leg")"
  bin="$(binary_for_leg "$leg")"

  if [[ "$EXTERNAL" == "true" ]]; then
    if [[ -n "$DEPLOY_HOOK" ]]; then
      log "deploy hook: leg=$leg phase=$phase drive_sync=$drive_sync"
      HOTPATH_ABBA_LEG="$leg" HOTPATH_ABBA_PHASE="$phase" HOTPATH_ABBA_BINARY="$bin" HOTPATH_ABBA_DRIVE_SYNC="$drive_sync" \
        run bash -c "$DEPLOY_HOOK"
    fi
    wait_health
    return 0
  fi

  local node_dir="$DATA_ROOT/$leg-sync-$drive_sync"
  local disks=() d
  for ((d = 1; d <= DISKS; d++)); do
    disks+=("$node_dir/d$d")
  done
  run mkdir -p "${disks[@]}"
  SERVER_LOG="$SERVER_LOG_DIR/$leg-sync-$drive_sync.log"

  if [[ "$DRY_RUN" == "true" ]]; then
    { printf 'DRY-RUN: RUSTFS_DRIVE_SYNC_ENABLE=%s %q server' "$drive_sync" "$bin"
      printf ' %q' "${disks[@]}"; printf '\n'; } >&2
    SERVER_PID="dry-run"
    return 0
  fi

  cat >"$SERVER_LOG_DIR/$leg-sync-$drive_sync.env" <<EOF
leg=$leg
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

measure() {
  local leg="$1" workload="$2" mode="$3" size="$4" sync_label="$5" baseline_csv="${6:-}"
  local cell="$OUT_DIR/$workload/$sync_label/$leg"
  local args=(
    --tool warp --warp-bin "$WARP_BIN" --warp-mode "$mode"
    --endpoint "$ADDRESS" --access-key "$ACCESS_KEY" --secret-key "$SECRET_KEY"
    --region "$REGION" --sizes "$size" --concurrency "$CONCURRENCY"
    --duration "$DURATION" --rounds "$ROUNDS" --cooldown-secs "$COOLDOWN_SECS"
    --out-dir "$cell"
  )
  [[ -n "$baseline_csv" ]] && args+=(--baseline-csv "$baseline_csv")
  run "$ENHANCED_BENCH" "${args[@]}" >&2
  echo "$cell"
}

write_schedule_header() {
  echo "sync_label,drive_sync,workload,mode,size,leg,phase,binary,out_dir" >"$OUT_DIR/abba_schedule.csv"
}

append_schedule() {
  local sync_label="$1" drive_sync="$2" workload="$3" mode="$4" size="$5" leg="$6"
  local phase bin
  phase="$(phase_for_leg "$leg")"
  bin="$(binary_for_leg "$leg")"
  echo "$sync_label,$drive_sync,$workload,$mode,$size,$leg,$phase,$bin,$OUT_DIR/$workload/$sync_label/$leg" >>"$OUT_DIR/abba_schedule.csv"
}

write_manifest() {
  cat >"$OUT_DIR/manifest.env" <<EOF
generated_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo unknown)
runner=$(uname -srm 2>/dev/null || echo unknown)
schedule=ABBA
rounds=$ROUNDS
duration=$DURATION
cooldown_secs=$COOLDOWN_SECS
concurrency=$CONCURRENCY
baseline_bin=$BASELINE_BIN
candidate_bin=$CANDIDATE_BIN
external=$EXTERNAL
endpoint=$ADDRESS
warp_version=$("$WARP_BIN" --version 2>/dev/null | head -n1 || echo unknown)
EOF
}

declare -a CANDIDATE_COMPARE_CSVS=()
declare -a DRIFT_COMPARE_CSVS=()

write_manifest
write_schedule_header

for ds_spec in "${DRIVE_SYNC_MATRIX[@]}"; do
  IFS='|' read -r sync_label drive_sync <<<"$ds_spec"

  for leg in A1 B1 B2 A2; do
    log "=== $sync_label leg $leg ($(phase_for_leg "$leg")) ==="
    bring_up "$leg" "$drive_sync"

    for wl_spec in "${WORKLOADS[@]}"; do
      IFS='|' read -r workload mode size <<<"$wl_spec"
      append_schedule "$sync_label" "$drive_sync" "$workload" "$mode" "$size" "$leg"

      baseline_csv=""
      if [[ "$leg" != "A1" ]]; then
        baseline_csv="$OUT_DIR/$workload/$sync_label/A1/median_summary.csv"
      fi

      cell="$(measure "$leg" "$workload" "$mode" "$size" "$sync_label" "$baseline_csv")"
      case "$leg" in
        B1|B2) CANDIDATE_COMPARE_CSVS+=("$cell/baseline_compare.csv") ;;
        A2) DRIFT_COMPARE_CSVS+=("$cell/baseline_compare.csv") ;;
      esac
    done

    tear_down
  done
done

gate_args=(--fail-pct "$FAIL_PCT" --warn-pct "$WARN_PCT" --markdown "$OUT_DIR/candidate_gate.md")
for csv in "${CANDIDATE_COMPARE_CSVS[@]}"; do
  gate_args+=(--compare-csv "$csv")
done
[[ "$ALLOW_REGRESSION" == "true" ]] && gate_args+=(--allow-regression --exemption-reason "$EXEMPTION_REASON")

drift_gate_args=(--fail-pct "$FAIL_PCT" --warn-pct "$WARN_PCT" --markdown "$OUT_DIR/baseline_drift_gate.md")
for csv in "${DRIFT_COMPARE_CSVS[@]}"; do
  drift_gate_args+=(--compare-csv "$csv")
done

if [[ "$DRY_RUN" == "true" ]]; then
  log "dry-run complete; candidate compare CSVs=${#CANDIDATE_COMPARE_CSVS[@]} baseline drift CSVs=${#DRIFT_COMPARE_CSVS[@]}"
  { printf 'DRY-RUN:'; printf ' %q' "$GATE" "${gate_args[@]}"; printf '\n'; } >&2
  { printf 'DRY-RUN:'; printf ' %q' "$GATE" "${drift_gate_args[@]}"; printf '\n'; } >&2
  exit 0
fi

log "applying candidate relative-budget gate"
set +e
"$GATE" "${gate_args[@]}"
candidate_status=$?
set -e

log "applying A2-vs-A1 baseline drift gate"
set +e
"$GATE" "${drift_gate_args[@]}"
drift_status=$?
set -e

cat >"$OUT_DIR/summary.md" <<EOF
# Hotpath Warp ABBA Summary

- schedule: A1 baseline -> B1 candidate -> B2 candidate -> A2 baseline
- runner: $(uname -srm 2>/dev/null || echo unknown)
- warp: $("$WARP_BIN" --version 2>/dev/null | head -n1 || echo unknown)
- matrix: duration=$DURATION rounds=$ROUNDS cooldown=$COOLDOWN_SECS disks=$DISKS concurrency=$CONCURRENCY
- endpoint: $ADDRESS
- baseline binary: $BASELINE_BIN
- candidate binary: $CANDIDATE_BIN
- candidate gate: $OUT_DIR/candidate_gate.md (exit $candidate_status)
- baseline drift gate: $OUT_DIR/baseline_drift_gate.md (exit $drift_status)

Interpretation:

- Treat candidate gate failures as actionable only when the A2-vs-A1 drift gate is PASS or the affected workload's A2 drift is materially smaller than the B1/B2 candidate delta.
- If both candidate and baseline drift fail on the same workload, rerun with longer duration, more rounds, or a quieter runner before assigning causality.
EOF

log "summary written to $OUT_DIR/summary.md"
if [[ "$candidate_status" -ne 0 || "$drift_status" -ne 0 ]]; then
  exit 1
fi
