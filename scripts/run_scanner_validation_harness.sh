#!/usr/bin/env bash
set -euo pipefail

ALIAS=""
ENDPOINT=""
ACCESS_KEY="${RUSTFS_ACCESS_KEY:-}"
SECRET_KEY=""
SECRET_KEY_ENV="RUSTFS_SECRET_KEY"
REGION="us-east-1"
DEPLOYMENT="single-disk"
WORKLOAD_LABEL="unspecified"
SAMPLES=30
INTERVAL_SECS=60
OUT_DIR=""
MC_BIN="mc"
AWSCURL_BIN="awscurl"
JQ_BIN="jq"
SKIP_HOST_TELEMETRY=false
RUSTFS_PID=""
TELEMETRY_PIDS=()

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_scanner_validation_harness.sh --alias <admin-alias> \
    --endpoint <url> [options]

Required:
  --alias                 Admin client alias used for config snapshots.
  --endpoint              RustFS endpoint, for example http://127.0.0.1:9000.
  RUSTFS_ACCESS_KEY       Admin access key for scanner status requests.
  RUSTFS_SECRET_KEY       Admin secret key for scanner status requests.

Optional:
  --access-key            Override RUSTFS_ACCESS_KEY for scanner status requests.
  --secret-key-env        Environment variable that stores the admin secret key
                          (default: RUSTFS_SECRET_KEY).
  --region                SigV4 region (default: us-east-1).
  --deployment            single-disk | multi-disk | distributed (default: single-disk).
  --workload-label        Free-form workload label written to metadata.
  --samples               Number of scanner status samples (default: 30).
  --interval-secs         Seconds between samples (default: 60).
  --out-dir               Output directory (default: target/bench/scanner-validation-<timestamp>).
  --mc-bin                mc-compatible admin client (default: mc).
  --awscurl-bin           SigV4 HTTP client (default: awscurl).
  --jq-bin                jq-compatible JSON processor (default: jq).
  --rustfs-pid            RustFS process id for pidstat. If omitted, pidof rustfs is used.
  --skip-host-telemetry   Do not run pidstat/iostat/mpstat.
  -h, --help              Show this help.

The harness collects scanner/heal config snapshots, scanner status samples,
host telemetry when available, and a compact scanner-summary.csv file. It does
not generate object workload or modify scanner configuration.
USAGE
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: command not found: $1" >&2
    exit 1
  fi
}

is_nonnegative_integer() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
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
      --alias) ALIAS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --endpoint) ENDPOINT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --access-key) ACCESS_KEY="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --secret-key-env) SECRET_KEY_ENV="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --region) REGION="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --deployment) DEPLOYMENT="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --workload-label) WORKLOAD_LABEL="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --samples) SAMPLES="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --interval-secs) INTERVAL_SECS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --out-dir) OUT_DIR="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --mc-bin) MC_BIN="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --awscurl-bin) AWSCURL_BIN="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --jq-bin) JQ_BIN="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --rustfs-pid) RUSTFS_PID="$(arg_value "$1" "${2:-}")"; shift 2 ;;
      --skip-host-telemetry) SKIP_HOST_TELEMETRY=true; shift ;;
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
  if [[ -z "$ALIAS" || -z "$ENDPOINT" || -z "$ACCESS_KEY" ]]; then
    echo "ERROR: --alias, --endpoint, and RUSTFS_ACCESS_KEY (or --access-key) are required" >&2
    exit 1
  fi

  if ! [[ "$SECRET_KEY_ENV" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "ERROR: --secret-key-env must be a valid environment variable name" >&2
    exit 1
  fi

  SECRET_KEY="${!SECRET_KEY_ENV:-}"
  if [[ -z "$SECRET_KEY" ]]; then
    echo "ERROR: $SECRET_KEY_ENV is required for scanner status requests" >&2
    exit 1
  fi

  case "$DEPLOYMENT" in
    single-disk|multi-disk|distributed) ;;
    *)
      echo "ERROR: --deployment must be single-disk, multi-disk, or distributed" >&2
      exit 1
      ;;
  esac

  if ! is_positive_integer "$SAMPLES"; then
    echo "ERROR: --samples must be a positive integer" >&2
    exit 1
  fi

  if ! is_nonnegative_integer "$INTERVAL_SECS"; then
    echo "ERROR: --interval-secs must be a nonnegative integer" >&2
    exit 1
  fi
}

setup_output() {
  if [[ -z "$OUT_DIR" ]]; then
    OUT_DIR="target/bench/scanner-validation-$(date -u +%Y%m%dT%H%M%SZ)"
  fi

  mkdir -p "$OUT_DIR/status"
  SUMMARY_CSV="$OUT_DIR/scanner-summary.csv"
  echo "timestamp,primary_pressure,current_cycle_objects_scanned,current_cycle_directories_scanned,last_cycle_result,last_cycle_partial_reason,last_cycle_partial_source,lifecycle_transition_scanner_missed,source_work_missed_total" >"$SUMMARY_CSV"
}

git_value() {
  local args=("$@")
  git "${args[@]}" 2>/dev/null || true
}

write_metadata() {
  local started_at="$1"

  {
    printf 'started_at=%s\n' "$started_at"
    printf 'deployment=%s\n' "$DEPLOYMENT"
    printf 'workload_label=%s\n' "$WORKLOAD_LABEL"
    printf 'endpoint=%s\n' "$ENDPOINT"
    printf 'region=%s\n' "$REGION"
    printf 'samples=%s\n' "$SAMPLES"
    printf 'interval_secs=%s\n' "$INTERVAL_SECS"
    printf 'git_commit=%s\n' "$(git_value rev-parse HEAD)"
    printf 'git_branch=%s\n' "$(git_value branch --show-current)"
  } >"$OUT_DIR/run-metadata.env"
}

capture_config_snapshots() {
  "$MC_BIN" admin config get "$ALIAS" scanner >"$OUT_DIR/scanner-config.before.txt"
  "$MC_BIN" admin config get "$ALIAS" heal >"$OUT_DIR/heal-config.before.txt"
}

first_rustfs_pid() {
  if [[ -n "$RUSTFS_PID" ]]; then
    echo "$RUSTFS_PID"
    return
  fi

  pidof rustfs 2>/dev/null | awk '{ print $1 }' || true
}

start_host_telemetry() {
  TELEMETRY_PIDS=()

  if [[ "$SKIP_HOST_TELEMETRY" == "true" || "$INTERVAL_SECS" == "0" ]]; then
    return
  fi

  local pid
  pid="$(first_rustfs_pid)"

  if [[ -n "$pid" ]] && command -v pidstat >/dev/null 2>&1; then
    pidstat -p "$pid" "$INTERVAL_SECS" "$SAMPLES" >"$OUT_DIR/pidstat.txt" 2>&1 &
    TELEMETRY_PIDS+=("$!")
  fi

  if command -v iostat >/dev/null 2>&1; then
    iostat -xz "$INTERVAL_SECS" "$SAMPLES" >"$OUT_DIR/iostat.txt" 2>&1 &
    TELEMETRY_PIDS+=("$!")
  fi

  if command -v mpstat >/dev/null 2>&1; then
    mpstat "$INTERVAL_SECS" "$SAMPLES" >"$OUT_DIR/mpstat.txt" 2>&1 &
    TELEMETRY_PIDS+=("$!")
  fi
}

wait_host_telemetry() {
  local pid

  if [[ ${#TELEMETRY_PIDS[@]} -eq 0 ]]; then
    return
  fi

  for pid in "${TELEMETRY_PIDS[@]}"; do
    wait "$pid" || true
  done
}

scanner_status_url() {
  printf '%s/rustfs/admin/v3/scanner/status\n' "${ENDPOINT%/}"
}

capture_status_sample() {
  local index="$1"
  local ts="$2"
  local status_file="$OUT_DIR/status/scanner-status.${index}.${ts}.json"

  AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
  AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
  AWS_DEFAULT_REGION="$REGION" \
  "$AWSCURL_BIN" \
    --service s3 \
    --region "$REGION" \
    --request GET \
    "$(scanner_status_url)" \
    | "$JQ_BIN" . >"$status_file"

  "$JQ_BIN" -r --arg ts "$ts" '
    [
      $ts,
      (.metrics.pacing_pressure.primary_pressure // ""),
      (.metrics.current_cycle_objects_scanned // 0),
      (.metrics.current_cycle_directories_scanned // 0),
      (.metrics.last_cycle_result // ""),
      (.metrics.last_cycle_partial_reason // ""),
      (.metrics.last_cycle_partial_source // ""),
      (.metrics.lifecycle_transition.scanner_missed // 0),
      ((.metrics.source_work // []) | map(.missed // 0) | add // 0)
    ] | @csv
  ' "$status_file" >>"$SUMMARY_CSV"
}

capture_status_series() {
  local index ts

  for ((index = 1; index <= SAMPLES; index++)); do
    ts="$(date -u +%Y%m%dT%H%M%SZ)"
    capture_status_sample "$index" "$ts"

    if [[ "$index" -lt "$SAMPLES" && "$INTERVAL_SECS" != "0" ]]; then
      sleep "$INTERVAL_SECS"
    fi
  done
}

main() {
  parse_args "$@"
  validate_args
  require_cmd "$MC_BIN"
  require_cmd "$AWSCURL_BIN"
  require_cmd "$JQ_BIN"
  setup_output

  local started_at
  started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  write_metadata "$started_at"
  capture_config_snapshots
  start_host_telemetry
  capture_status_series
  wait_host_telemetry

  echo "Scanner validation artifacts written to $OUT_DIR"
}

main "$@"
