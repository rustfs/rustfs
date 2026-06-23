#!/usr/bin/env bash
set -euo pipefail

# Issue #708 tuning matrix runner for large-object PUT.
# This controller:
# - defines high-priority tuning profiles for 16MiB / 32MiB PUT
# - writes an env snapshot per profile
# - optionally calls an apply hook after switching profile env
# - reuses scripts/run_put_large_stage_breakdown_with_capture.sh for each profile

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_SCRIPT="${SCRIPT_DIR}/run_put_large_stage_breakdown_with_capture.sh"

GROUP="all" # all|baseline|inflight|io-buffer|runtime|duplex
ENDPOINT=""
ACCESS_KEY=""
SECRET_KEY=""
REGION="us-east-1"
BUCKET_PREFIX="rustfs-put-large"
CONCURRENCIES="16,32,64,96,128"
SIZES="16MiB,32MiB"
DURATION="120s"
ROUNDS=3
OUT_ROOT=""
WARP_BIN="warp"
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

CAPTURE_METRICS_ENDPOINTS=""
CAPTURE_PROM_METRICS_URLS=""
CAPTURE_INTERVAL_SECS=15
CAPTURE_RUSTFS_PID=""
CAPTURE_SKIP_HOST_TELEMETRY=false

APPLY_CMD=""
APPLY_CMD_ARR=()
APPLY_WAIT_SECS=20

BASE_OBJECT_IO_BUFFER_SIZE=262144
BASE_OBJECT_DUPLEX_BUFFER_SIZE=8388608
BASE_ERASURE_ENCODE_MAX_INFLIGHT_BYTES=25165824
BASE_RUNTIME_WORKER_THREADS=12
BASE_RUNTIME_MAX_BLOCKING_THREADS=512

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_put_large_tuning_matrix.sh --endpoint <url> \
    --access-key <ak> --secret-key <sk> [options]

Required:
  --endpoint <url>
  --access-key <ak>
  --secret-key <sk>

Core options:
  --group <name>                     all|baseline|inflight|io-buffer|runtime|duplex
  --bucket-prefix <prefix>           Default: rustfs-put-large
  --region <name>                    Default: us-east-1
  --concurrencies <csv>              Default: 16,32,64,96,128
  --sizes <csv>                      Default: 16MiB,32MiB
  --duration <dur>                   Default: 120s
  --rounds <n>                       Default: 3
  --out-root <dir>                   Default: target/bench/put-large-tuning-<timestamp>
  --warp-bin <path>                  Default: warp
  --extra-args "<args>"
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

Capture options:
  --capture-metrics-endpoints <csv>
  --capture-prom-metrics-urls <csv>
  --capture-interval-secs <n>        Default: 15
  --capture-rustfs-pid <pid>
  --capture-skip-host-telemetry

Optional apply hook:
  --apply-cmd "<cmd>"                Optional plain command + args, no shell operators.
                                     Useful for restarting/reloading RustFS after env changes.
  --apply-wait-secs <n>              Wait after apply command (default: 20)

Behavior:
  - Each profile writes: <out-root>/<profile>/env_snapshot.env
  - Each profile then calls scripts/run_put_large_stage_breakdown_with_capture.sh
  - Non-baseline profiles automatically compare against <out-root>/baseline
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

parse_apply_cmd() {
  local raw="$1"
  if [[ "$raw" == *';'* || "$raw" == *'&&'* || "$raw" == *'||'* || "$raw" == *'|'* || "$raw" == *'<'* || "$raw" == *'>'* || "$raw" == *'`'* || "$raw" == *'$'* ]]; then
    echo "ERROR: --apply-cmd does not allow shell operators or expansions; pass a plain command and args only" >&2
    exit 1
  fi
  IFS=$' \t\n' read -r -a APPLY_CMD_ARR <<< "$raw"
  if [[ "${#APPLY_CMD_ARR[@]}" -eq 0 ]]; then
    echo "ERROR: --apply-cmd must not be empty" >&2
    exit 1
  fi
  require_cmd "${APPLY_CMD_ARR[0]}"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --endpoint) ENDPOINT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --access-key) ACCESS_KEY="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --secret-key) SECRET_KEY="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --group) GROUP="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --bucket-prefix) BUCKET_PREFIX="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --region) REGION="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --concurrencies) CONCURRENCIES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --sizes) SIZES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --duration) DURATION="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --rounds) ROUNDS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --out-root) OUT_ROOT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --warp-bin) WARP_BIN="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --extra-args) EXTRA_ARGS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --nodes) TOPOLOGY_NODES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --disks-per-node) TOPOLOGY_DISKS_PER_NODE="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --total-disks) TOPOLOGY_TOTAL_DISKS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --cpu-per-node) TOPOLOGY_CPU_PER_NODE="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --mem-per-node) TOPOLOGY_MEM_PER_NODE="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --network) TOPOLOGY_NETWORK="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --endpoint-mode) TOPOLOGY_ENDPOINT_MODE="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --erasure-set-drive-count) TOPOLOGY_ERASURE_SET_DRIVE_COUNT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --client-host) CLIENT_HOST="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --capture-metrics-endpoints) CAPTURE_METRICS_ENDPOINTS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --capture-prom-metrics-urls) CAPTURE_PROM_METRICS_URLS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --capture-interval-secs) CAPTURE_INTERVAL_SECS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --capture-rustfs-pid) CAPTURE_RUSTFS_PID="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --capture-skip-host-telemetry) CAPTURE_SKIP_HOST_TELEMETRY=true; shift ;;
      --apply-cmd) APPLY_CMD="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --apply-wait-secs) APPLY_WAIT_SECS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
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

validate_args() {
  if [[ -z "$ENDPOINT" || -z "$ACCESS_KEY" || -z "$SECRET_KEY" ]]; then
    echo "ERROR: --endpoint, --access-key, and --secret-key are required" >&2
    exit 1
  fi
  case "$GROUP" in
    all|baseline|inflight|io-buffer|runtime|duplex) ;;
    *) echo "ERROR: --group must be all|baseline|inflight|io-buffer|runtime|duplex" >&2; exit 1 ;;
  esac
  if ! is_positive_int "$ROUNDS" || ! is_positive_int "$CAPTURE_INTERVAL_SECS" || ! is_positive_int "$APPLY_WAIT_SECS"; then
    echo "ERROR: --rounds, --capture-interval-secs, and --apply-wait-secs must be positive integers" >&2
    exit 1
  fi
  if [[ -n "$APPLY_CMD" ]]; then
    parse_apply_cmd "$APPLY_CMD"
  fi
}

setup_out_root() {
  if [[ -z "$OUT_ROOT" ]]; then
    OUT_ROOT="target/bench/put-large-tuning-$(date -u +%Y%m%dT%H%M%SZ)"
  fi
  mkdir -p "$OUT_ROOT"
}

apply_profile() {
  local profile="$1"
  export RUSTFS_OBJECT_IO_BUFFER_SIZE="$BASE_OBJECT_IO_BUFFER_SIZE"
  export RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE="$BASE_OBJECT_DUPLEX_BUFFER_SIZE"
  export RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES="$BASE_ERASURE_ENCODE_MAX_INFLIGHT_BYTES"
  export RUSTFS_RUNTIME_WORKER_THREADS="$BASE_RUNTIME_WORKER_THREADS"
  export RUSTFS_RUNTIME_MAX_BLOCKING_THREADS="$BASE_RUNTIME_MAX_BLOCKING_THREADS"

  case "$profile" in
    baseline) ;;
    inflight-32m) export RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES=33554432 ;;
    inflight-48m) export RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES=50331648 ;;
    inflight-64m) export RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES=67108864 ;;
    io-buffer-512k) export RUSTFS_OBJECT_IO_BUFFER_SIZE=524288 ;;
    io-buffer-1m) export RUSTFS_OBJECT_IO_BUFFER_SIZE=1048576 ;;
    runtime-w16-b768)
      export RUSTFS_RUNTIME_WORKER_THREADS=16
      export RUSTFS_RUNTIME_MAX_BLOCKING_THREADS=768
      ;;
    runtime-w20-b1024)
      export RUSTFS_RUNTIME_WORKER_THREADS=20
      export RUSTFS_RUNTIME_MAX_BLOCKING_THREADS=1024
      ;;
    duplex-16m) export RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=16777216 ;;
    *)
      echo "ERROR: unsupported profile $profile" >&2
      exit 1
      ;;
  esac
}

write_env_snapshot() {
  local out_file="$1"
  cat > "$out_file" <<EOF
RUSTFS_OBJECT_IO_BUFFER_SIZE=${RUSTFS_OBJECT_IO_BUFFER_SIZE}
RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=${RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE}
RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES=${RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES}
RUSTFS_RUNTIME_WORKER_THREADS=${RUSTFS_RUNTIME_WORKER_THREADS}
RUSTFS_RUNTIME_MAX_BLOCKING_THREADS=${RUSTFS_RUNTIME_MAX_BLOCKING_THREADS}
EOF
}

run_apply_hook_if_needed() {
  local profile="$1"
  local env_file="$2"
  if [[ "${#APPLY_CMD_ARR[@]}" -eq 0 ]]; then
    return
  fi
  echo "[${profile}] running apply command..."
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[DRY-RUN] RUSTFS_TUNING_ENV_FILE=%q ' "$env_file"
    printf '%q ' "${APPLY_CMD_ARR[@]}"
    printf '\n'
    echo "[DRY-RUN] sleep $APPLY_WAIT_SECS"
  else
    RUSTFS_TUNING_ENV_FILE="$env_file" "${APPLY_CMD_ARR[@]}"
    echo "[${profile}] waiting ${APPLY_WAIT_SECS}s for service readiness..."
    sleep "$APPLY_WAIT_SECS"
  fi
}

profiles_for_group() {
  case "$1" in
    baseline) echo "baseline" ;;
    inflight) echo "baseline inflight-32m inflight-48m inflight-64m" ;;
    io-buffer) echo "baseline io-buffer-512k io-buffer-1m" ;;
    runtime) echo "baseline runtime-w16-b768 runtime-w20-b1024" ;;
    duplex) echo "baseline duplex-16m" ;;
    all) echo "baseline inflight-32m inflight-48m inflight-64m io-buffer-512k io-buffer-1m runtime-w16-b768 runtime-w20-b1024 duplex-16m" ;;
  esac
}

run_profile() {
  local profile="$1"
  local out_dir env_file baseline_root
  out_dir="${OUT_ROOT}/${profile}"
  mkdir -p "$out_dir"

  apply_profile "$profile"
  env_file="${out_dir}/env_snapshot.env"
  write_env_snapshot "$env_file"
  run_apply_hook_if_needed "$profile" "$env_file"

  baseline_root=""
  if [[ "$profile" != "baseline" ]]; then
    baseline_root="${OUT_ROOT}/baseline"
  fi

  local -a cmd=(
    bash "$RUNNER_SCRIPT"
    --endpoint "$ENDPOINT"
    --access-key "$ACCESS_KEY"
    --secret-key "$SECRET_KEY"
    --bucket-prefix "${BUCKET_PREFIX}-${profile}"
    --region "$REGION"
    --concurrencies "$CONCURRENCIES"
    --sizes "$SIZES"
    --duration "$DURATION"
    --rounds "$ROUNDS"
    --out-dir "$out_dir"
    --warp-bin "$WARP_BIN"
  )

  if [[ -n "$baseline_root" ]]; then
    cmd+=(--baseline-root "$baseline_root")
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
  if [[ -n "$CAPTURE_METRICS_ENDPOINTS" ]]; then
    cmd+=(--capture-metrics-endpoints "$CAPTURE_METRICS_ENDPOINTS")
  fi
  if [[ -n "$CAPTURE_PROM_METRICS_URLS" ]]; then
    cmd+=(--capture-prom-metrics-urls "$CAPTURE_PROM_METRICS_URLS")
  fi
  if [[ -n "$CAPTURE_RUSTFS_PID" ]]; then
    cmd+=(--capture-rustfs-pid "$CAPTURE_RUSTFS_PID")
  fi
  if [[ "$CAPTURE_SKIP_HOST_TELEMETRY" == "true" ]]; then
    cmd+=(--capture-skip-host-telemetry)
  fi
  if [[ "$INSECURE" == "true" ]]; then
    cmd+=(--insecure)
  fi
  if [[ "$DRY_RUN" == "true" ]]; then
    cmd+=(--dry-run)
  fi

  echo
  echo "===== Running profile ${profile} ====="
  echo "Output: $out_dir"
  echo "Env snapshot: $env_file"
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[DRY-RUN] '
    printf '%q ' "${cmd[@]}"
    printf '\n'
  else
    "${cmd[@]}"
  fi
}

main() {
  parse_args "$@"
  validate_args
  require_cmd bash
  require_cmd awk
  if [[ ! -x "$RUNNER_SCRIPT" && ! -f "$RUNNER_SCRIPT" ]]; then
    echo "ERROR: missing dependency script: $RUNNER_SCRIPT" >&2
    exit 1
  fi
  setup_out_root

  echo "Tuning output root: $OUT_ROOT"
  echo "Group: $GROUP"
  echo "Concurrencies: $CONCURRENCIES"
  echo "Sizes: $SIZES"

  local profile
  for profile in $(profiles_for_group "$GROUP"); do
    run_profile "$profile"
  done

  echo
  echo "Done. Profile outputs are under: $OUT_ROOT"
}

main "$@"
