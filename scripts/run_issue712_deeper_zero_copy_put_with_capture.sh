#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNNER_SCRIPT="${PROJECT_ROOT}/scripts/run_put_large_stage_breakdown_with_capture.sh"

ENDPOINT="${ENDPOINT:-http://127.0.0.1:9000}"
ACCESS_KEY="${ACCESS_KEY:-}"
SECRET_KEY="${SECRET_KEY:-}"
REGION="${REGION:-us-east-1}"
SIZES="${SIZES:-64MiB,128MiB,256MiB}"
CONCURRENCIES="${CONCURRENCIES:-16}"
DURATION="${DURATION:-60s}"
ROUNDS="${ROUNDS:-1}"
COOLDOWN_SECS="${COOLDOWN_SECS:-15}"
OUT_DIR="${OUT_DIR:-target/bench/issue712-deeper-zero-copy-capture-$(date -u +%Y%m%dT%H%M%SZ)}"
CAPTURE_INTERVAL_SECS="${CAPTURE_INTERVAL_SECS:-15}"
CAPTURE_PROM_METRICS_URLS="${CAPTURE_PROM_METRICS_URLS:-http://127.0.0.1:8889/metrics}"
CAPTURE_RUSTFS_PID="${CAPTURE_RUSTFS_PID:-}"
WORKLOAD_LABEL="${WORKLOAD_LABEL:-issue-712-deeper-zero-copy}"
DRY_RUN=false

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_issue712_deeper_zero_copy_put_with_capture.sh \
    --access-key <ak> --secret-key <sk> [options]

Required:
  --access-key <ak>
  --secret-key <sk>

Options:
  --endpoint <url>                 Default: http://127.0.0.1:9000
  --region <name>                  Default: us-east-1
  --sizes <csv>                    Default: 64MiB,128MiB,256MiB
  --concurrencies <csv>            Default: 16
  --duration <dur>                 Default: 60s
  --rounds <n>                     Default: 1
  --cooldown-secs <n>              Default: 15
  --out-dir <dir>                  Default: target/bench/issue712-deeper-zero-copy-capture-<timestamp>
  --capture-interval-secs <n>      Default: 15
  --capture-prom-metrics-urls <csv>
                                   Default: http://127.0.0.1:8889/metrics
  --capture-rustfs-pid <pid>       Optional explicit rustfs pid
  --dry-run
  -h, --help

Notes:
  - This wrapper assumes the local RustFS server is already running with:
      RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST=true
  - It reuses scripts/run_put_large_stage_breakdown_with_capture.sh
    and only narrows the matrix to the deeper-zero-copy focus area.
USAGE
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
      --region) REGION="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --sizes) SIZES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --concurrencies) CONCURRENCIES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --duration) DURATION="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --rounds) ROUNDS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --cooldown-secs) COOLDOWN_SECS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --out-dir) OUT_DIR="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --capture-interval-secs) CAPTURE_INTERVAL_SECS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --capture-prom-metrics-urls) CAPTURE_PROM_METRICS_URLS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --capture-rustfs-pid) CAPTURE_RUSTFS_PID="$(arg_value "$1" "${2:-}")"; shift 2 ;;
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

validate_args() {
  if [[ -z "$ACCESS_KEY" || -z "$SECRET_KEY" ]]; then
    echo "ERROR: --access-key and --secret-key are required" >&2
    exit 1
  fi
}

main() {
  parse_args "$@"
  validate_args

  local -a cmd=(
    bash "$RUNNER_SCRIPT"
    --endpoint "$ENDPOINT"
    --access-key "$ACCESS_KEY"
    --secret-key "$SECRET_KEY"
    --region "$REGION"
    --sizes "$SIZES"
    --concurrencies "$CONCURRENCIES"
    --duration "$DURATION"
    --rounds "$ROUNDS"
    --retry-per-round 1
    --retry-sleep-secs 2
    --cooldown-secs "$COOLDOWN_SECS"
    --out-dir "$OUT_DIR"
    --workload-label "$WORKLOAD_LABEL"
    --capture-label deeper-zero-copy-window
    --capture-interval-secs "$CAPTURE_INTERVAL_SECS"
    --capture-prom-metrics-urls "$CAPTURE_PROM_METRICS_URLS"
  )

  if [[ -n "$CAPTURE_RUSTFS_PID" ]]; then
    cmd+=(--capture-rustfs-pid "$CAPTURE_RUSTFS_PID")
  fi

  if [[ "$DRY_RUN" == "true" ]]; then
    cmd+=(--dry-run)
  fi

  printf 'Command:'
  printf ' %q' "${cmd[@]}"
  printf '\n'

  "${cmd[@]}"
}

main "$@"
