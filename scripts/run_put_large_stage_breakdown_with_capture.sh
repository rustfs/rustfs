#!/usr/bin/env bash
set -euo pipefail

# One-shot wrapper for issue #706:
# - runs the large PUT stage-breakdown benchmark matrix
# - starts a parallel artifact collector that covers the benchmark window
# - stores both outputs under the same run root

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BENCH_SCRIPT="${PROJECT_ROOT}/scripts/run_put_large_stage_breakdown.sh"
CAPTURE_SCRIPT="${PROJECT_ROOT}/scripts/collect_put_large_stage_breakdown_artifacts.sh"

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
OUT_DIR=""
BASELINE_ROOT=""
EXTRA_ARGS=""
INSECURE=false
DRY_RUN=false

WARP_BIN="${WARP_BIN:-warp}"

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

CAPTURE_LABEL="benchmark-window"
CAPTURE_METRICS_ENDPOINTS=""
CAPTURE_PROM_METRICS_URLS=""
CAPTURE_DURATION_SECS=""
CAPTURE_INTERVAL_SECS=15
CAPTURE_RUSTFS_PID=""
CAPTURE_SKIP_HOST_TELEMETRY=false

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_put_large_stage_breakdown_with_capture.sh --endpoint <url> \
    --access-key <ak> --secret-key <sk> [options]

Benchmark options:
  --endpoint <url>
  --access-key <ak>
  --secret-key <sk>
  --bucket-prefix <prefix>            Default: rustfs-put-large
  --region <name>                     Default: us-east-1
  --sizes <csv>                       Default: 16MiB,32MiB
  --concurrencies <csv>               Default: 16,32,64,96,128
  --duration <dur>                    Default: 120s
  --rounds <n>                        Default: 3
  --retry-per-round <n>               Default: 1
  --retry-sleep-secs <n>              Default: 2
  --out-dir <dir>                     Default: target/bench/put-large-stage-breakdown-<timestamp>
  --baseline-root <dir>
  --extra-args "<args>"
  --warp-bin <path>                   Default: warp
  --insecure
  --dry-run

Topology metadata:
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

Capture options:
  --capture-label <name>              Default: benchmark-window
  --capture-metrics-endpoints <csv>   Signed admin metrics endpoints
  --capture-prom-metrics-urls <csv>   Optional plain Prometheus text endpoints
  --capture-duration-secs <n>         Override auto-derived capture window
  --capture-interval-secs <n>         Default: 15
  --capture-rustfs-pid <pid>
  --capture-skip-host-telemetry

Behavior:
  - If --capture-duration-secs is omitted, the wrapper derives a nominal
    capture window from the benchmark matrix and adds a safety slack.
  - The collector runs in the background while the benchmark matrix is in the foreground.
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
      --capture-label) CAPTURE_LABEL="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --capture-metrics-endpoints) CAPTURE_METRICS_ENDPOINTS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --capture-prom-metrics-urls) CAPTURE_PROM_METRICS_URLS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --capture-duration-secs) CAPTURE_DURATION_SECS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --capture-interval-secs) CAPTURE_INTERVAL_SECS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --capture-rustfs-pid) CAPTURE_RUSTFS_PID="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --capture-skip-host-telemetry) CAPTURE_SKIP_HOST_TELEMETRY=true; shift ;;
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

count_csv_items() {
  local csv="$1"
  local count=0 raw item
  IFS=',' read -r -a arr <<< "$csv"
  for raw in "${arr[@]}"; do
    item="$(echo "$raw" | awk '{$1=$1;print}')"
    [[ -z "$item" ]] && continue
    count=$((count + 1))
  done
  echo "$count"
}

duration_to_seconds() {
  local value="$1"
  if [[ "$value" =~ ^([0-9]+)s$ ]]; then
    echo "${BASH_REMATCH[1]}"
  elif [[ "$value" =~ ^([0-9]+)m$ ]]; then
    echo $(( BASH_REMATCH[1] * 60 ))
  elif [[ "$value" =~ ^([0-9]+)h$ ]]; then
    echo $(( BASH_REMATCH[1] * 3600 ))
  elif [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "$value"
  else
    echo "ERROR"
  fi
}

validate_args() {
  if [[ -z "$ENDPOINT" || -z "$ACCESS_KEY" || -z "$SECRET_KEY" ]]; then
    echo "ERROR: --endpoint, --access-key, and --secret-key are required" >&2
    exit 1
  fi
  if ! is_positive_int "$ROUNDS" || ! is_positive_int "$RETRY_PER_ROUND" || ! is_positive_int "$RETRY_SLEEP_SECS"; then
    echo "ERROR: --rounds, --retry-per-round, and --retry-sleep-secs must be positive integers" >&2
    exit 1
  fi
  if ! is_positive_int "$CAPTURE_INTERVAL_SECS"; then
    echo "ERROR: --capture-interval-secs must be a positive integer" >&2
    exit 1
  fi
  if [[ -n "$CAPTURE_DURATION_SECS" && ! "$CAPTURE_DURATION_SECS" =~ ^[0-9]+$ ]]; then
    echo "ERROR: --capture-duration-secs must be a nonnegative integer" >&2
    exit 1
  fi
  if [[ -n "$BASELINE_ROOT" && ! -d "$BASELINE_ROOT" ]]; then
    echo "ERROR: --baseline-root does not exist: $BASELINE_ROOT" >&2
    exit 1
  fi
  local duration_secs
  duration_secs="$(duration_to_seconds "$DURATION")"
  if [[ "$duration_secs" == "ERROR" ]]; then
    echo "ERROR: unsupported --duration format: $DURATION (expected e.g. 120s, 2m, 1h)" >&2
    exit 1
  fi
}

setup_output() {
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="target/bench/put-large-stage-breakdown-$(date -u +%Y%m%dT%H%M%SZ)"
  fi
}

derive_capture_duration() {
  if [[ -n "$CAPTURE_DURATION_SECS" ]]; then
    echo "$CAPTURE_DURATION_SECS"
    return
  fi

  local sizes_count conc_count duration_secs nominal bench_secs retry_slack fixed_slack
  sizes_count="$(count_csv_items "$SIZES")"
  conc_count="$(count_csv_items "$CONCURRENCIES")"
  duration_secs="$(duration_to_seconds "$DURATION")"
  bench_secs=$(( sizes_count * conc_count * ROUNDS * duration_secs ))
  retry_slack=$(( conc_count * RETRY_PER_ROUND * RETRY_SLEEP_SECS + conc_count * 30 ))
  fixed_slack=120
  nominal=$(( bench_secs + retry_slack + fixed_slack ))
  echo "$nominal"
}

run_capture() {
  local capture_duration="$1"
  local -a cmd=(
    bash "$CAPTURE_SCRIPT"
    --run-root "$OUT_DIR"
    --endpoint "$ENDPOINT"
    --label "$CAPTURE_LABEL"
    --access-key "$ACCESS_KEY"
    --secret-key-env RUSTFS_CAPTURE_SECRET_KEY
    --region "$REGION"
    --duration-secs "$capture_duration"
    --interval-secs "$CAPTURE_INTERVAL_SECS"
  )

  if [[ -n "$CAPTURE_METRICS_ENDPOINTS" ]]; then
    cmd+=(--metrics-endpoints "$CAPTURE_METRICS_ENDPOINTS")
  fi
  if [[ -n "$CAPTURE_PROM_METRICS_URLS" ]]; then
    cmd+=(--prom-metrics-urls "$CAPTURE_PROM_METRICS_URLS")
  fi
  if [[ -n "$CAPTURE_RUSTFS_PID" ]]; then
    cmd+=(--rustfs-pid "$CAPTURE_RUSTFS_PID")
  fi
  if [[ "$CAPTURE_SKIP_HOST_TELEMETRY" == "true" ]]; then
    cmd+=(--skip-host-telemetry)
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    cmd+=(--dry-run)
  fi

  printf 'Capture command:'
  printf ' %q' "${cmd[@]}"
  printf '\n'

  export RUSTFS_CAPTURE_SECRET_KEY="$SECRET_KEY"
  "${cmd[@]}" &
  CAPTURE_PID=$!
}

run_benchmark() {
  local -a cmd=(
    bash "$BENCH_SCRIPT"
    --endpoint "$ENDPOINT"
    --access-key "$ACCESS_KEY"
    --secret-key "$SECRET_KEY"
    --bucket-prefix "$BUCKET_PREFIX"
    --region "$REGION"
    --sizes "$SIZES"
    --concurrencies "$CONCURRENCIES"
    --duration "$DURATION"
    --rounds "$ROUNDS"
    --retry-per-round "$RETRY_PER_ROUND"
    --retry-sleep-secs "$RETRY_SLEEP_SECS"
    --out-dir "$OUT_DIR"
    --warp-bin "$WARP_BIN"
    --workload-label "$WORKLOAD_LABEL"
  )

  if [[ -n "$BASELINE_ROOT" ]]; then
    cmd+=(--baseline-root "$BASELINE_ROOT")
  fi
  if [[ -n "$EXTRA_ARGS" ]]; then
    cmd+=(--extra-args "$EXTRA_ARGS")
  fi
  if [[ -n "$TOPOLOGY_NODES" ]]; then
    cmd+=(--nodes "$TOPOLOGY_NODES")
  fi
  if [[ -n "$TOPOLOGY_DISKS_PER_NODE" ]]; then
    cmd+=(--disks-per-node "$TOPOLOGY_DISKS_PER_NODE")
  fi
  if [[ -n "$TOPOLOGY_TOTAL_DISKS" ]]; then
    cmd+=(--total-disks "$TOPOLOGY_TOTAL_DISKS")
  fi
  if [[ -n "$TOPOLOGY_CPU_PER_NODE" ]]; then
    cmd+=(--cpu-per-node "$TOPOLOGY_CPU_PER_NODE")
  fi
  if [[ -n "$TOPOLOGY_MEM_PER_NODE" ]]; then
    cmd+=(--mem-per-node "$TOPOLOGY_MEM_PER_NODE")
  fi
  if [[ -n "$TOPOLOGY_NETWORK" ]]; then
    cmd+=(--network "$TOPOLOGY_NETWORK")
  fi
  if [[ -n "$TOPOLOGY_ENDPOINT_MODE" ]]; then
    cmd+=(--endpoint-mode "$TOPOLOGY_ENDPOINT_MODE")
  fi
  if [[ -n "$TOPOLOGY_ERASURE_SET_DRIVE_COUNT" ]]; then
    cmd+=(--erasure-set-drive-count "$TOPOLOGY_ERASURE_SET_DRIVE_COUNT")
  fi
  if [[ -n "$CLIENT_HOST" ]]; then
    cmd+=(--client-host "$CLIENT_HOST")
  fi
  if [[ "$INSECURE" == "true" ]]; then
    cmd+=(--insecure)
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    cmd+=(--dry-run)
  fi

  printf 'Benchmark command:'
  printf ' %q' "${cmd[@]}"
  printf '\n'

  "${cmd[@]}"
}

cleanup_capture() {
  if [[ -n "${CAPTURE_PID:-}" ]] && kill -0 "$CAPTURE_PID" >/dev/null 2>&1; then
    kill "$CAPTURE_PID" >/dev/null 2>&1 || true
    wait "$CAPTURE_PID" >/dev/null 2>&1 || true
  fi
}

main() {
  parse_args "$@"
  validate_args
  require_cmd bash
  require_cmd awk
  require_cmd "$WARP_BIN"

  setup_output
  local capture_duration
  capture_duration="$(derive_capture_duration)"

  echo "Output dir: $OUT_DIR"
  echo "Derived capture duration seconds: $capture_duration"
  echo "Capture label: $CAPTURE_LABEL"

  trap cleanup_capture EXIT
  run_capture "$capture_duration"
  run_benchmark
  wait "$CAPTURE_PID"
  CAPTURE_PID=""

  echo
  echo "One-shot benchmark + capture run finished."
  echo "Run root: $OUT_DIR"
}

main "$@"
