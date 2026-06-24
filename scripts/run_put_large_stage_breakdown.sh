#!/usr/bin/env bash
set -euo pipefail

# Large-object PUT stage-breakdown runner for issue #706.
# This wrapper standardizes:
# - object sizes and concurrency matrix for 16MiB / 32MiB PUT runs
# - output directory layout under target/bench/
# - artifact naming and run manifest capture
# - optional baseline comparison reuse across per-concurrency runs

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENHANCED_BENCH_SCRIPT="${PROJECT_ROOT}/scripts/run_object_batch_bench_enhanced.sh"

TOOL="warp"
ENDPOINT=""
ACCESS_KEY=""
SECRET_KEY=""
REGION="us-east-1"
BUCKET_PREFIX="rustfs-put-large"
CONCURRENCIES="16,32,64,96,128"
SIZES="16MiB,32MiB"
DURATION="120s"
ROUNDS=3
RETRY_PER_ROUND=1
RETRY_SLEEP_SECS=2
COOLDOWN_SECS=0
WARP_BIN="${WARP_BIN:-warp}"
OUT_DIR=""
BASELINE_ROOT=""
EXTRA_ARGS=""
INSECURE=false
DRY_RUN=false

TOPOLOGY_NODES=""
TOPOLOGY_DISKS_PER_NODE=""
TOPOLOGY_TOTAL_DISKS=""
TOPOLOGY_CPU_PER_NODE=""
TOPOLOGY_MEM_PER_NODE=""
TOPOLOGY_NETWORK=""
TOPOLOGY_ENDPOINT_MODE=""
TOPOLOGY_ERASURE_SET_DRIVE_COUNT=""
CLIENT_HOST=""
WORKLOAD_LABEL="issue-706-large-put-stage-breakdown"

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_put_large_stage_breakdown.sh --endpoint <url> \
    --access-key <ak> --secret-key <sk> [options]

Required:
  --endpoint <url>                    S3 endpoint
  --access-key <ak>                   Access key
  --secret-key <sk>                   Secret key

Core options:
  --bucket-prefix <prefix>            Bucket prefix (default: rustfs-put-large)
  --region <name>                     Region (default: us-east-1)
  --sizes <csv>                       Object sizes (default: 16MiB,32MiB)
  --concurrencies <csv>               Concurrency matrix (default: 16,32,64,96,128)
  --duration <dur>                    Per-run duration (default: 120s)
  --rounds <n>                        Rounds per size (default: 3)
  --retry-per-round <n>               Retries per failed round (default: 1)
  --retry-sleep-secs <n>              Sleep between retries (default: 2)
  --cooldown-secs <n>                 Sleep between rounds/sizes (default: 0)
  --out-dir <dir>                     Output root (default: target/bench/put-large-stage-breakdown-<timestamp>)
  --baseline-root <dir>               Existing root from a previous run of this script
  --extra-args "<args>"               Extra args passed to run_object_batch_bench_enhanced.sh
  --warp-bin <path>                   warp binary (default: warp)
  --insecure                          Pass --insecure to warp
  --dry-run                           Print commands only

Topology metadata (optional but recommended):
  --nodes <n>
  --disks-per-node <n>
  --total-disks <n>
  --cpu-per-node <text>
  --mem-per-node <text>
  --network <text>
  --endpoint-mode <direct|lb>
  --erasure-set-drive-count <n>
  --client-host <text>
  --workload-label <text>

Output layout:
  <out-dir>/
    run_manifest.txt
    run_matrix.csv
    aggregate_median_summary.csv
    aggregate_baseline_compare.csv
    artifact_layout.txt
    runs/
      c16/
      c32/
      ...

Each runs/cXX directory is a direct output directory of:
  scripts/run_object_batch_bench_enhanced.sh
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: command not found: $1" >&2
    exit 1
  fi
}

arg_value() {
  local flag="$1"
  local value="${2:-}"
  if [[ -z "$value" || "$value" == --* ]]; then
    echo "ERROR: missing value for $flag" >&2
    exit 1
  fi
  printf '%s\n' "$value"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --endpoint) ENDPOINT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --access-key) ACCESS_KEY="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --secret-key) SECRET_KEY="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --bucket-prefix) BUCKET_PREFIX="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --region) REGION="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --sizes) SIZES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --concurrencies) CONCURRENCIES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --duration) DURATION="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --rounds) ROUNDS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --retry-per-round) RETRY_PER_ROUND="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --retry-sleep-secs) RETRY_SLEEP_SECS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --cooldown-secs) COOLDOWN_SECS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --out-dir) OUT_DIR="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --baseline-root) BASELINE_ROOT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --extra-args) EXTRA_ARGS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --warp-bin) WARP_BIN="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --nodes) TOPOLOGY_NODES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --disks-per-node) TOPOLOGY_DISKS_PER_NODE="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --total-disks) TOPOLOGY_TOTAL_DISKS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --cpu-per-node) TOPOLOGY_CPU_PER_NODE="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --mem-per-node) TOPOLOGY_MEM_PER_NODE="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --network) TOPOLOGY_NETWORK="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --endpoint-mode) TOPOLOGY_ENDPOINT_MODE="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --erasure-set-drive-count) TOPOLOGY_ERASURE_SET_DRIVE_COUNT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --client-host) CLIENT_HOST="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --workload-label) WORKLOAD_LABEL="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --insecure) INSECURE=true; shift ;;
      --dry-run) DRY_RUN=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *)
        echo "ERROR: unknown arg: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

is_positive_int() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

is_nonnegative_int() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

cooldown_sleep() {
  local secs="$1"
  if (( secs <= 0 )); then
    return 0
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY-RUN] sleep ${secs}"
  else
    sleep "$secs"
  fi
}

validate_args() {
  if [[ -z "$ENDPOINT" || -z "$ACCESS_KEY" || -z "$SECRET_KEY" ]]; then
    echo "ERROR: --endpoint, --access-key, and --secret-key are required" >&2
    exit 1
  fi
  if [[ "$TOOL" != "warp" ]]; then
    echo "ERROR: only warp is supported by this wrapper" >&2
    exit 1
  fi
  if ! is_positive_int "$ROUNDS" || ! is_positive_int "$RETRY_PER_ROUND" || ! is_positive_int "$RETRY_SLEEP_SECS" || ! is_nonnegative_int "$COOLDOWN_SECS"; then
    echo "ERROR: --rounds, --retry-per-round, and --retry-sleep-secs must be positive integers; --cooldown-secs must be a nonnegative integer" >&2
    exit 1
  fi
  if [[ -n "$BASELINE_ROOT" && ! -d "$BASELINE_ROOT" ]]; then
    echo "ERROR: --baseline-root does not exist: $BASELINE_ROOT" >&2
    exit 1
  fi
}

setup_output() {
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="target/bench/put-large-stage-breakdown-$(date -u +%Y%m%dT%H%M%SZ)"
  fi
  RUNS_DIR="${OUT_DIR}/runs"
  AGG_MEDIAN_CSV="${OUT_DIR}/aggregate_median_summary.csv"
  AGG_COMPARE_CSV="${OUT_DIR}/aggregate_baseline_compare.csv"
  RUN_MATRIX_CSV="${OUT_DIR}/run_matrix.csv"
  mkdir -p "$RUNS_DIR"

  echo "concurrency,size,tool,successful_rounds,failed_rounds,median_throughput_bps,median_reqps,median_latency_ms,bucket,run_dir" > "$AGG_MEDIAN_CSV"
  echo "concurrency,size,tool,new_median_reqps,baseline_median_reqps,delta_reqps_pct,new_median_latency_ms,baseline_median_latency_ms,delta_latency_pct,new_median_throughput_bps,baseline_median_throughput_bps,delta_throughput_pct,run_dir" > "$AGG_COMPARE_CSV"
  echo "concurrency,bucket,run_dir,baseline_csv,status" > "$RUN_MATRIX_CSV"
}

join_bool() {
  if [[ "$1" == "true" ]]; then
    echo "true"
  else
    echo "false"
  fi
}

write_manifest() {
  local git_commit git_branch git_dirty rustc_version
  git_commit="$(git -C "$PROJECT_ROOT" rev-parse HEAD 2>/dev/null || echo "unknown")"
  git_branch="$(git -C "$PROJECT_ROOT" branch --show-current 2>/dev/null || echo "unknown")"
  if [[ -n "$(git -C "$PROJECT_ROOT" status --porcelain 2>/dev/null || true)" ]]; then
    git_dirty="true"
  else
    git_dirty="false"
  fi
  rustc_version="$(rustc --version 2>/dev/null || echo "unknown")"

  {
    echo "created_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "git_commit=${git_commit}"
    echo "git_branch=${git_branch}"
    echo "git_dirty=${git_dirty}"
    echo "rustc_version=${rustc_version}"
    echo "kernel=$(uname -srvmo 2>/dev/null || echo "unknown")"
    echo "tool=${TOOL}"
    echo "endpoint=${ENDPOINT}"
    echo "region=${REGION}"
    echo "bucket_prefix=${BUCKET_PREFIX}"
    echo "sizes=${SIZES}"
    echo "concurrencies=${CONCURRENCIES}"
    echo "duration=${DURATION}"
    echo "rounds=${ROUNDS}"
    echo "retry_per_round=${RETRY_PER_ROUND}"
    echo "retry_sleep_secs=${RETRY_SLEEP_SECS}"
    echo "cooldown_secs=${COOLDOWN_SECS}"
    echo "insecure=$(join_bool "$INSECURE")"
    echo "dry_run=$(join_bool "$DRY_RUN")"
    echo "baseline_root=${BASELINE_ROOT:-N/A}"
    echo "extra_args_present=$([[ -n "$EXTRA_ARGS" ]] && echo true || echo false)"
    echo "workload_label=${WORKLOAD_LABEL}"
    echo "nodes=${TOPOLOGY_NODES:-N/A}"
    echo "disks_per_node=${TOPOLOGY_DISKS_PER_NODE:-N/A}"
    echo "total_disks=${TOPOLOGY_TOTAL_DISKS:-N/A}"
    echo "cpu_per_node=${TOPOLOGY_CPU_PER_NODE:-N/A}"
    echo "mem_per_node=${TOPOLOGY_MEM_PER_NODE:-N/A}"
    echo "network=${TOPOLOGY_NETWORK:-N/A}"
    echo "endpoint_mode=${TOPOLOGY_ENDPOINT_MODE:-N/A}"
    echo "erasure_set_drive_count=${TOPOLOGY_ERASURE_SET_DRIVE_COUNT:-N/A}"
    echo "client_host=${CLIENT_HOST:-N/A}"
    echo "access_key=REDACTED"
    echo "secret_key=REDACTED"
  } > "${OUT_DIR}/run_manifest.txt"
}

write_artifact_layout() {
  cat > "${OUT_DIR}/artifact_layout.txt" <<'EOF'
Top-level artifacts:
- run_manifest.txt: run metadata, git revision, topology notes, and redacted execution context
- run_matrix.csv: one row per concurrency run, including bucket and baseline linkage
- aggregate_median_summary.csv: merged median_summary rows from every concurrency run
- aggregate_baseline_compare.csv: merged baseline_compare rows when a matching baseline exists
- runs/cXX/: direct output directory of scripts/run_object_batch_bench_enhanced.sh

Per-concurrency directory:
- round_results.csv
- median_summary.csv
- baseline_compare.csv (only when a baseline CSV is supplied)
- logs/
EOF
}

trim() {
  echo "$1" | awk '{$1=$1;print}'
}

csv_to_lines() {
  local csv="$1"
  IFS=',' read -r -a arr <<< "$csv"
  for raw in "${arr[@]}"; do
    local item
    item="$(trim "$raw")"
    [[ -z "$item" ]] && continue
    echo "$item"
  done
}

sanitize_bucket_component() {
  echo "$1" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9-]+/-/g; s/^-+//; s/-+$//; s/-{2,}/-/g'
}

build_bucket_name() {
  local concurrency="$1"
  local run_id prefix raw
  run_id="$(date -u +%Y%m%d%H%M%S)"
  prefix="$(sanitize_bucket_component "$BUCKET_PREFIX")"
  raw="${prefix}-${run_id}-c${concurrency}"
  raw="$(sanitize_bucket_component "$raw")"
  echo "${raw:0:63}"
}

resolve_baseline_csv() {
  local concurrency="$1"
  local candidate=""
  if [[ -z "$BASELINE_ROOT" ]]; then
    return 0
  fi

  if [[ -f "${BASELINE_ROOT}/runs/c${concurrency}/median_summary.csv" ]]; then
    candidate="${BASELINE_ROOT}/runs/c${concurrency}/median_summary.csv"
  elif [[ -f "${BASELINE_ROOT}/c${concurrency}/median_summary.csv" ]]; then
    candidate="${BASELINE_ROOT}/c${concurrency}/median_summary.csv"
  fi

  if [[ -n "$candidate" ]]; then
    printf '%s\n' "$candidate"
  fi
}

append_aggregate_rows() {
  local concurrency="$1"
  local bucket="$2"
  local run_dir="$3"
  local median_csv="$4"
  local compare_csv="$5"

  awk -F',' -v c="$concurrency" -v b="$bucket" -v rd="$run_dir" 'NR>1 {print c "," $1 "," $2 "," $4 "," $5 "," $6 "," $7 "," $8 "," b "," rd}' "$median_csv" >> "$AGG_MEDIAN_CSV"

  if [[ -f "$compare_csv" ]]; then
    awk -F',' -v c="$concurrency" -v rd="$run_dir" 'NR>1 {print c "," $1 "," $2 "," $4 "," $5 "," $6 "," $7 "," $8 "," $9 "," $10 "," $11 "," $12 "," rd}' "$compare_csv" >> "$AGG_COMPARE_CSV"
  fi
}

run_concurrency() {
  local concurrency="$1"
  local run_dir bucket baseline_csv status
  run_dir="${RUNS_DIR}/c${concurrency}"
  bucket="$(build_bucket_name "$concurrency")"
  baseline_csv="$(resolve_baseline_csv "$concurrency" || true)"
  status="pending"

  local -a cmd=(
    bash "$ENHANCED_BENCH_SCRIPT"
    --tool "$TOOL"
    --endpoint "$ENDPOINT"
    --access-key "$ACCESS_KEY"
    --secret-key "$SECRET_KEY"
    --bucket "$bucket"
    --region "$REGION"
    --warp-bin "$WARP_BIN"
    --warp-mode put
    --sizes "$SIZES"
    --concurrency "$concurrency"
    --duration "$DURATION"
    --rounds "$ROUNDS"
    --retry-per-round "$RETRY_PER_ROUND"
    --retry-sleep-secs "$RETRY_SLEEP_SECS"
    --cooldown-secs "$COOLDOWN_SECS"
    --out-dir "$run_dir"
  )

  if [[ "$INSECURE" == "true" ]]; then
    cmd+=(--insecure)
  fi
  if [[ -n "$baseline_csv" ]]; then
    cmd+=(--baseline-csv "$baseline_csv")
  fi
  if [[ -n "$EXTRA_ARGS" ]]; then
    cmd+=(--extra-args "$EXTRA_ARGS")
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    cmd+=(--dry-run)
  fi

  echo "==== concurrency=${concurrency} bucket=${bucket} ===="
  printf 'Command:'
  printf ' %q' "${cmd[@]}"
  printf '\n'

  if "${cmd[@]}"; then
    status="ok"
  else
    status="failed"
  fi

  echo "${concurrency},${bucket},${run_dir},${baseline_csv:-N/A},${status}" >> "$RUN_MATRIX_CSV"

  if [[ "$status" != "ok" ]]; then
    echo "ERROR: concurrency=${concurrency} failed; aborting remaining matrix runs" >&2
    exit 1
  fi

  append_aggregate_rows "$concurrency" "$bucket" "$run_dir" "${run_dir}/median_summary.csv" "${run_dir}/baseline_compare.csv"
}

main() {
  parse_args "$@"
  validate_args
  require_cmd bash
  require_cmd git
  require_cmd awk
  require_cmd sed
  require_cmd sort
  require_cmd "$WARP_BIN"

  if [[ ! -x "$ENHANCED_BENCH_SCRIPT" && ! -f "$ENHANCED_BENCH_SCRIPT" ]]; then
    echo "ERROR: missing dependency script: $ENHANCED_BENCH_SCRIPT" >&2
    exit 1
  fi

  setup_output
  write_manifest
  write_artifact_layout

  echo "Output dir: $OUT_DIR"
  echo "Concurrencies: $CONCURRENCIES"
  echo "Sizes: $SIZES"
  echo "Duration: $DURATION"
  echo "Rounds: $ROUNDS"
  echo "Cooldown secs: $COOLDOWN_SECS"

  local conc_count conc_index
  conc_count="$(csv_to_lines "$CONCURRENCIES" | awk 'END{print NR+0}')"
  conc_index=0
  while IFS= read -r concurrency; do
    conc_index=$(( conc_index + 1 ))
    run_concurrency "$concurrency"
    if (( COOLDOWN_SECS > 0 && conc_index < conc_count )); then
      echo "Cooldown after concurrency=${concurrency}: ${COOLDOWN_SECS}s"
      cooldown_sleep "$COOLDOWN_SECS"
    fi
  done < <(csv_to_lines "$CONCURRENCIES")

  echo
  echo "Stage-breakdown run finished."
  echo "Artifacts written to: $OUT_DIR"
  echo "Top-level summaries:"
  echo "  - $RUN_MATRIX_CSV"
  echo "  - $AGG_MEDIAN_CSV"
  if [[ -s "$AGG_COMPARE_CSV" ]]; then
    echo "  - $AGG_COMPARE_CSV"
  fi
}

main "$@"
