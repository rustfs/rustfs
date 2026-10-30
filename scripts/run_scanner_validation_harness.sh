#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if [[ "${1:-}" == "--abba" ]]; then
  shift
  exec "$SCRIPT_DIR/python_bin.sh" "$SCRIPT_DIR/scanner_abba.py" "$@"
fi

ALIAS=""
ENDPOINT=""
ACCESS_KEY="${RUSTFS_ACCESS_KEY:-}"
SECRET_KEY=""
SECRET_KEY_ENV="RUSTFS_SECRET_KEY"
REGION="us-east-1"
DEPLOYMENT="single-disk"
WORKLOAD_LABEL="unspecified"
METRICS_ENDPOINTS=""
SAMPLES=30
INTERVAL_SECS=60
OUT_DIR=""
MC_BIN="mc"
AWSCURL_BIN="awscurl"
JQ_BIN="jq"
SKIP_HOST_TELEMETRY=false
RUSTFS_PID=""
TELEMETRY_PIDS=()
TELEMETRY_NAMES=()
CAPTURE_PIDS=()
HEAL_STATUS_FILES=()
CAPTURE_CLOCK_ID=""

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_scanner_validation_harness.sh --abba --help
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
  --metrics-endpoints     Optional comma-separated RustFS endpoints for
                          per-endpoint background-heal status and
                          /realtime?types=1&by-host=true&n=1 capture.
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
background heal status samples, optional distributed by-host admin metrics,
host telemetry when available, a compact scanner-summary.csv file, and a
scanner-validation-report.md file. It does not generate object workload or
modify scanner configuration.
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
      --metrics-endpoints) METRICS_ENDPOINTS="$(arg_value "$1" "${2:-}")"; shift 2 ;;
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
  mkdir -p "$OUT_DIR/heal"
  mkdir -p "$OUT_DIR/metrics"
  mkdir -p "$OUT_DIR/requests"
  rm -f "$OUT_DIR/pidstat.txt" "$OUT_DIR/iostat.txt" "$OUT_DIR/mpstat.txt" \
    "$OUT_DIR/host-telemetry-status.tsv"
  printf 'collector\texit_status\n' >"$OUT_DIR/host-telemetry-status.tsv"
  SUMMARY_CSV="$OUT_DIR/scanner-summary.csv"
  echo "timestamp,primary_pressure,current_cycle_objects_scanned,current_cycle_directories_scanned,last_cycle_result,last_cycle_partial_reason,last_cycle_partial_source,lifecycle_transition_scanner_missed,source_work_missed_total,current_cycle_usage_saves,last_cycle_usage_saves,usage_dirty_pending_buckets,usage_last_cycle_dirty_buckets,usage_last_cycle_cleared_dirty_buckets,usage_last_save_result,usage_last_save_unix_secs,life_time_scan_cycle,life_time_scan_bucket_drive,life_time_scan_object,life_time_save_usage,heal_queue_length,heal_active_tasks,heal_scanner_queued,heal_admin_queued,heal_auto_heal_queued" >"$SUMMARY_CSV"
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
    printf 'metrics_endpoints=%s\n' "$METRICS_ENDPOINTS"
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
  TELEMETRY_NAMES=()

  if [[ "$SKIP_HOST_TELEMETRY" == "true" || "$INTERVAL_SECS" == "0" ]]; then
    return
  fi

  local pid
  pid="$(first_rustfs_pid)"

  if [[ -n "$pid" ]] && command -v pidstat >/dev/null 2>&1; then
    pidstat -p "$pid" "$INTERVAL_SECS" "$SAMPLES" >"$OUT_DIR/pidstat.txt" 2>&1 &
    TELEMETRY_PIDS+=("$!")
    TELEMETRY_NAMES+=("pidstat")
  fi

  if command -v iostat >/dev/null 2>&1; then
    if [[ "$(uname -s)" == "Darwin" ]]; then
      iostat -d -w "$INTERVAL_SECS" -c "$SAMPLES" >"$OUT_DIR/iostat.txt" 2>&1 &
    else
      iostat -xz "$INTERVAL_SECS" "$SAMPLES" >"$OUT_DIR/iostat.txt" 2>&1 &
    fi
    TELEMETRY_PIDS+=("$!")
    TELEMETRY_NAMES+=("iostat")
  fi

  if command -v mpstat >/dev/null 2>&1; then
    mpstat "$INTERVAL_SECS" "$SAMPLES" >"$OUT_DIR/mpstat.txt" 2>&1 &
    TELEMETRY_PIDS+=("$!")
    TELEMETRY_NAMES+=("mpstat")
  fi
}

finalize_host_telemetry() {
  local run_status="${1:-0}" index pid status write_status=0

  if [[ ${#TELEMETRY_PIDS[@]} -eq 0 ]]; then
    return
  fi

  local collector_pids=("${TELEMETRY_PIDS[@]}")
  local collector_names=("${TELEMETRY_NAMES[@]}")
  TELEMETRY_PIDS=()
  TELEMETRY_NAMES=()

  if [[ "$run_status" != "0" ]]; then
    for pid in "${collector_pids[@]}"; do
      kill "$pid" 2>/dev/null || true
    done
  fi

  for index in "${!collector_pids[@]}"; do
    if wait "${collector_pids[$index]}"; then
      status=0
    else
      status=$?
      printf 'WARN: host telemetry collector %s exited with status %s\n' "${collector_names[$index]}" "$status" >&2 || true
    fi
    printf '%s\t%s\n' "${collector_names[$index]}" "$status" >>"$OUT_DIR/host-telemetry-status.tsv" || write_status=$?
  done
  return "$write_status"
}

scanner_status_url() {
  printf '%s/rustfs/admin/v3/scanner/status\n' "${ENDPOINT%/}"
}

background_heal_status_url() {
  printf '%s/rustfs/admin/v3/background-heal/status\n' "${1%/}"
}

metrics_url() {
  printf '%s/rustfs/admin/v3/realtime?types=1&by-host=true&n=1\n' "${1%/}"
}

endpoint_label() {
  local endpoint="$1"
  local label

  label="${endpoint#*://}"
  label="${label%%/*}"
  printf '%s' "$label" | tr -c 'A-Za-z0-9._-' '_'
}

background_heal_status_endpoints() {
  local endpoint
  local endpoints=()
  local emitted=false

  if [[ -n "$METRICS_ENDPOINTS" ]]; then
    IFS=',' read -r -a endpoints <<<"$METRICS_ENDPOINTS"
    for endpoint in "${endpoints[@]}"; do
      if [[ -z "$endpoint" ]]; then
        continue
      fi
      printf '%s\n' "$endpoint"
      emitted=true
    done
  fi

  if [[ "$emitted" != "true" ]]; then
    printf '%s\n' "$ENDPOINT"
  fi
}

capture_http_request() {
  local method="$1" url="$2" output="$3"

  # Run only in the background: exec makes the tracked PID the request owner.
  AWS_ACCESS_KEY_ID="$ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$SECRET_KEY" AWS_DEFAULT_REGION="$REGION" \
    exec "$SCRIPT_DIR/python_bin.sh" - "$SCRIPT_DIR" "$AWSCURL_BIN" "$REGION" "$method" "$url" \
      "$output" "$OUT_DIR/requests/$(basename "$output").timing.json" "$CAPTURE_CLOCK_ID" <<'PY'
import json
from pathlib import Path
import signal
import subprocess
import sys
import time

sys.path.insert(0, sys.argv[1])
from scanner_abba import OwnedCommand

_, _, binary, region, method, url, output, timing, clock = sys.argv

def interrupted(signum, _frame):
    raise SystemExit(128 + signum)

for sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(sig, interrupted)

process = None
status = 1
started = time.monotonic_ns()
started_unix = time.time_ns()
try:
    with Path(output).open("wb") as stream:
        process = OwnedCommand([binary, "--service", "s3", "--region", region, "--request", method, url], stream)
        status = process.wait(300)
except subprocess.TimeoutExpired:
    status = 124
except SystemExit as error:
    status = error.code
finally:
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, signal.SIG_IGN)
    try:
        if process is not None:
            try:
                if process.status != 0:
                    # collect_live already owns the cancellation grace period.
                    # Kill this nested session before its owner is escalated,
                    # including descendants of an exited failing command.
                    process._signal_group(signal.SIGKILL)
            finally:
                process.finish()
    finally:
        ended = time.monotonic_ns()
        ended_unix = time.time_ns()
        Path(timing).write_text(json.dumps({
            "clock_id": clock, "start_ns": started, "end_ns": ended,
            "start_unix_ns": started_unix, "end_unix_ns": ended_unix,
            "exit_status": status, "synchronized_source_window": False,
        }) + "\n")
raise SystemExit(status if status >= 0 else 128 - status)
PY
}

wait_capture_requests() {
  local index pid status=0 request_status
  if [[ ${#CAPTURE_PIDS[@]} -eq 0 ]]; then return; fi
  for index in "${!CAPTURE_PIDS[@]}"; do
    pid="${CAPTURE_PIDS[$index]}"
    if wait "$pid"; then
      :
    else
      request_status=$?
      if [[ "$status" == 0 ]]; then status="$request_status"; fi
    fi
    unset "CAPTURE_PIDS[$index]"
  done
  CAPTURE_PIDS=()
  return "$status"
}

stop_capture_requests() {
  local pid
  if [[ ${#CAPTURE_PIDS[@]} -eq 0 ]]; then return; fi
  for pid in "${CAPTURE_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait_capture_requests || true
}

write_heal_status_summary() {
  local summary_file="$1"
  shift

  "$JQ_BIN" -s '
    {
      healOperations: {
        queueLength: (map(.healOperations.queueLength // .healQueueLength // 0) | add // 0),
        activeTasks: (map(.healOperations.activeTasks // .healActiveTasks // 0) | add // 0),
        queuedBySource: {
          scanner: (map(.healOperations.queuedBySource.scanner // 0) | add // 0),
          admin: (map(.healOperations.queuedBySource.admin // 0) | add // 0),
          autoHeal: (map(.healOperations.queuedBySource.autoHeal // 0) | add // 0),
          internal: (map(.healOperations.queuedBySource.internal // 0) | add // 0)
        },
        activeBySource: {
          scanner: (map(.healOperations.activeBySource.scanner // 0) | add // 0),
          admin: (map(.healOperations.activeBySource.admin // 0) | add // 0),
          autoHeal: (map(.healOperations.activeBySource.autoHeal // 0) | add // 0),
          internal: (map(.healOperations.activeBySource.internal // 0) | add // 0)
        },
        queuedByPriority: {
          low: (map(.healOperations.queuedByPriority.low // 0) | add // 0),
          normal: (map(.healOperations.queuedByPriority.normal // 0) | add // 0),
          high: (map(.healOperations.queuedByPriority.high // 0) | add // 0),
          urgent: (map(.healOperations.queuedByPriority.urgent // 0) | add // 0)
        },
        activeByPriority: {
          low: (map(.healOperations.activeByPriority.low // 0) | add // 0),
          normal: (map(.healOperations.activeByPriority.normal // 0) | add // 0),
          high: (map(.healOperations.activeByPriority.high // 0) | add // 0),
          urgent: (map(.healOperations.activeByPriority.urgent // 0) | add // 0)
        }
      }
    }
  ' "$@" >"$summary_file"
}

capture_background_heal_status_sample() {
  local index="$1"
  local ts="$2"
  local endpoint label heal_status_file
  HEAL_STATUS_FILES=()

  while IFS= read -r endpoint; do
    label="$(endpoint_label "$endpoint")"
    heal_status_file="$OUT_DIR/heal/background-heal-status.${label}.${index}.${ts}.json"
    capture_http_request POST "$(background_heal_status_url "$endpoint")" "$heal_status_file" &
    CAPTURE_PIDS+=("$!")
    HEAL_STATUS_FILES+=("$heal_status_file")
  done < <(background_heal_status_endpoints)
}

capture_distributed_metrics_sample() {
  local index="$1"
  local ts="$2"
  local endpoint label metrics_file
  local endpoints=()

  if [[ -z "$METRICS_ENDPOINTS" ]]; then
    return
  fi

  IFS=',' read -r -a endpoints <<<"$METRICS_ENDPOINTS"
  for endpoint in "${endpoints[@]}"; do
    if [[ -z "$endpoint" ]]; then
      continue
    fi

    label="$(endpoint_label "$endpoint")"
    metrics_file="$OUT_DIR/metrics/admin-metrics.${label}.${index}.${ts}.ndjson"
    capture_http_request GET "$(metrics_url "$endpoint")" "$metrics_file" &
    CAPTURE_PIDS+=("$!")
  done
}

capture_status_sample() {
  local index="$1"
  local ts="$2"
  local status_file="$OUT_DIR/status/scanner-status.${index}.${ts}.json"
  local heal_summary_file="$OUT_DIR/heal/background-heal-summary.${index}.${ts}.json"

  capture_http_request GET "$(scanner_status_url)" "$status_file" &
  CAPTURE_PIDS+=("$!")
  capture_background_heal_status_sample "$index" "$ts"
  capture_distributed_metrics_sample "$index" "$ts"
  wait_capture_requests
  write_heal_status_summary "$heal_summary_file" "${HEAL_STATUS_FILES[@]}"

  "$JQ_BIN" -r --arg ts "$ts" --slurpfile heal_status "$heal_summary_file" '
    ($heal_status[0] // {}) as $heal |
    [
      $ts,
      (.metrics.pacing_pressure.primary_pressure // ""),
      (.metrics.current_cycle_objects_scanned // 0),
      (.metrics.current_cycle_directories_scanned // 0),
      (.metrics.last_cycle_result // ""),
      (.metrics.last_cycle_partial_reason // ""),
      (.metrics.last_cycle_partial_source // ""),
      (.metrics.lifecycle_transition.scanner_missed // 0),
      ((.metrics.source_work // []) | map(.missed // 0) | add // 0),
      (.metrics.current_cycle_usage_saves // 0),
      (.metrics.last_cycle_usage_saves // 0),
      (.metrics.usage_freshness.dirty_pending_buckets // 0),
      (.metrics.usage_freshness.last_cycle_dirty_buckets // 0),
      (.metrics.usage_freshness.last_cycle_cleared_dirty_buckets // 0),
      (.metrics.usage_freshness.last_usage_save_result // ""),
      (.metrics.usage_freshness.last_usage_save_unix_secs // 0),
      (.metrics.life_time_ops.scan_cycle // 0),
      (.metrics.life_time_ops.scan_bucket_drive // 0),
      (.metrics.life_time_ops.scan_object // 0),
      (.metrics.life_time_ops.save_usage // 0),
      ($heal.healOperations.queueLength // $heal.healQueueLength // 0),
      ($heal.healOperations.activeTasks // $heal.healActiveTasks // 0),
      ($heal.healOperations.queuedBySource.scanner // 0),
      ($heal.healOperations.queuedBySource.admin // 0),
      ($heal.healOperations.queuedBySource.autoHeal // 0)
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

count_artifacts() {
  local dir="$1"
  local pattern="$2"

  find "$dir" -type f -name "$pattern" 2>/dev/null | wc -l | tr -d ' '
}

write_report() {
  local status_count heal_status_count metrics_count
  status_count="$(count_artifacts "$OUT_DIR/status" 'scanner-status.*.json')"
  heal_status_count="$(count_artifacts "$OUT_DIR/heal" 'background-heal-status.*.json')"
  metrics_count="$(count_artifacts "$OUT_DIR/metrics" 'admin-metrics.*.ndjson')"

  cat >"$OUT_DIR/scanner-validation-report.md" <<EOF
## Scanner Validation Report

Deployment: $DEPLOYMENT
Workload label: $WORKLOAD_LABEL
Endpoint: $ENDPOINT
Samples: $SAMPLES
Interval seconds: $INTERVAL_SECS

## Artifact Summary

- Scanner status snapshots: $status_count
- Background heal status snapshots: $heal_status_count
- Distributed admin metrics snapshots: $metrics_count
- Summary CSV: scanner-summary.csv
- Usage freshness summary: current/last usage saves, dirty bucket state, last usage save result, and scanner life_time_ops.
- Run metadata: run-metadata.env
- HTTP request timing: requests/*.timing.json (one client clock; actual request intervals, not synchronized server metric windows).

## Review Checklist

- Compare scanner progress and pressure in scanner-summary.csv.
- Confirm dirty usage marks lead to usage saves when validating post-start bucket metrics freshness.
- Check life_time_ops scan_cycle, scan_bucket_drive, scan_object, and save_usage before accepting bucket metrics freshness results.
- Check source work missed totals before accepting pressure reductions.
- Check healOperations queued and active counts when heal or bitrot pressure is involved.
- Use distributed admin metrics snapshots for by-host investigation when metrics endpoints were provided.
- Attach host telemetry when pidstat, iostat, or mpstat files are present.
EOF
}

main() {
  parse_args "$@"
  validate_args
  require_cmd "$MC_BIN"
  require_cmd "$AWSCURL_BIN"
  require_cmd "$JQ_BIN"
  CAPTURE_CLOCK_ID=$("$SCRIPT_DIR/python_bin.sh" -c 'import uuid; print(uuid.uuid4())')
  setup_output

  local started_at
  started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  write_metadata "$started_at"
  capture_config_snapshots
  trap 'harness_exit_status=$?; stop_capture_requests; finalize_host_telemetry "$harness_exit_status" || true; exit "$harness_exit_status"' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  start_host_telemetry
  capture_status_series
  finalize_host_telemetry
  write_report

  echo "Scanner validation artifacts written to $OUT_DIR"
}

main "$@"
